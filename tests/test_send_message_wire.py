"""
Wire-level tests for SMTP.send_message.

An in-memory server receives everything the client writes (no sockets are
bound and no mail service is started), so tests can assert on the exact
MAIL/RCPT parameters and the raw DATA bytes transmitted.
"""

import asyncio
import email.parser
import email.policy
import re
from collections.abc import Iterable
from email.message import EmailMessage, Message

import pytest

from aiosmtplib import SMTP, SMTPNotSupported
from aiosmtplib.protocol import SMTPProtocol


DATA_TERMINATOR = b"\r\n.\r\n"
BCC_HEADER_REGEX = re.compile(rb"^\s*bcc\s*:", flags=re.IGNORECASE | re.MULTILINE)
RESENT_BCC_HEADER_REGEX = re.compile(
    rb"^\s*resent-bcc\s*:", flags=re.IGNORECASE | re.MULTILINE
)


class InMemorySMTPServer:
    """
    A minimal in-memory SMTP server.

    Records every command (verb + raw argument bytes) and every complete
    DATA block (including the terminating CRLF.CRLF), and replies to the
    client synchronously via its protocol.
    """

    def __init__(self, extensions: Iterable[str] = ("8BITMIME", "SMTPUTF8")) -> None:
        self.extensions = list(extensions)
        self.commands: list[tuple[str, bytes]] = []
        self.data_blocks: list[bytes] = []
        self.protocol: SMTPProtocol | None = None
        self._line_buffer = bytearray()
        self._data_buffer: bytearray | None = None

    def feed(self, data: bytes) -> None:
        if self._data_buffer is not None:
            self._data_buffer.extend(data)
            end = self._data_buffer.find(DATA_TERMINATOR)
            if end != -1:
                self.data_blocks.append(
                    bytes(self._data_buffer[: end + len(DATA_TERMINATOR)])
                )
                self._data_buffer = None
                self._respond(b"250 2.0.0 OK\r\n")
            return

        self._line_buffer.extend(data)
        while True:
            line_end = self._line_buffer.find(b"\r\n")
            if line_end == -1:
                break
            line = bytes(self._line_buffer[:line_end])
            del self._line_buffer[: line_end + 2]
            self._handle_command(line)

    def _handle_command(self, line: bytes) -> None:
        verb, _, args = line.partition(b" ")
        command = verb.decode("ascii").upper()
        self.commands.append((command, args))

        if command == "EHLO":
            self._respond(self._ehlo_response())
        elif command == "HELO":
            self._respond(b"250 testserver\r\n")
        elif command == "DATA":
            self._data_buffer = bytearray()
            self._respond(b"354 End data with <CR><LF>.<CR><LF>\r\n")
        elif command == "QUIT":
            self._respond(b"221 2.0.0 Bye\r\n")
        else:
            self._respond(b"250 2.1.0 OK\r\n")

    def _ehlo_response(self) -> bytes:
        if not self.extensions:
            return b"250 testserver\r\n"
        lines = [b"250-testserver"]
        lines.extend(b"250-" + ext.encode("ascii") for ext in self.extensions[:-1])
        lines.append(b"250 " + self.extensions[-1].encode("ascii"))
        return b"\r\n".join(lines) + b"\r\n"

    def _respond(self, data: bytes) -> None:
        assert self.protocol is not None
        self.protocol.data_received(data)

    @property
    def command_verbs(self) -> list[str]:
        return [verb for verb, _ in self.commands]

    @property
    def mail_args(self) -> list[bytes]:
        return [args for verb, args in self.commands if verb == "MAIL"]

    @property
    def rcpt_args(self) -> list[bytes]:
        return [args for verb, args in self.commands if verb == "RCPT"]


class InMemoryTransport(asyncio.Transport):
    """Hands everything the client writes straight to the in-memory server."""

    def __init__(self, server: InMemorySMTPServer) -> None:
        super().__init__()
        self.server = server
        self._closing = False

    def write(self, data: bytes) -> None:
        self.server.feed(data)

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._closing = True

    def get_extra_info(self, name: str, default: object = None) -> object:
        return default


def make_connected_client(server: InMemorySMTPServer) -> SMTP:
    """Build an SMTP client wired to the in-memory server, without connect()."""
    client = SMTP(hostname="testserver")
    protocol = SMTPProtocol(loop=asyncio.get_running_loop())
    transport = InMemoryTransport(server)
    server.protocol = protocol
    protocol.connection_made(transport)
    client.protocol = protocol
    client.transport = transport
    return client


def parse_data_block(
    block: bytes, policy: email.policy.Policy = email.policy.default
) -> Message:
    assert block.endswith(DATA_TERMINATOR)
    raw_message = block[: -len(DATA_TERMINATOR)]
    # Parse from str so non-ASCII (SMTPUTF8) headers decode correctly;
    # BytesParser would surface raw UTF-8 bytes as surrogate escapes.
    return email.parser.Parser(policy=policy).parsestr(raw_message.decode("utf-8"))


def build_message() -> EmailMessage:
    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = "to@example.com"
    message["Subject"] = "test"
    message.set_content("hello")
    return message


async def test_bcc_recipients_go_to_rcpt_but_not_data() -> None:
    server = InMemorySMTPServer()
    client = make_connected_client(server)

    message = build_message()
    message["Cc"] = "cc@example.com"
    message["Bcc"] = "hidden1@example.com, hidden2@example.com"

    errors, _ = await client.send_message(message)

    assert not errors
    assert server.command_verbs == [
        "EHLO",
        "MAIL",
        "RCPT",
        "RCPT",
        "RCPT",
        "RCPT",
        "DATA",
    ]
    assert server.mail_args == [b"FROM:<sender@example.com> BODY=8BITMIME"]
    assert server.rcpt_args == [
        b"TO:<to@example.com>",
        b"TO:<cc@example.com>",
        b"TO:<hidden1@example.com>",
        b"TO:<hidden2@example.com>",
    ]

    assert len(server.data_blocks) == 1
    data = server.data_blocks[0]
    parsed = parse_data_block(data)
    assert parsed["Bcc"] is None
    assert parsed["To"] == "to@example.com"
    assert parsed["Cc"] == "cc@example.com"
    assert BCC_HEADER_REGEX.search(data) is None
    assert b"hidden1@example.com" not in data
    assert b"hidden2@example.com" not in data

    # The caller's message object keeps all of its fields.
    assert str(message["Bcc"]) == "hidden1@example.com, hidden2@example.com"
    assert str(message["To"]) == "to@example.com"
    assert str(message["Cc"]) == "cc@example.com"


async def test_resent_block_determines_envelope_and_strips_resent_bcc() -> None:
    server = InMemorySMTPServer()
    client = make_connected_client(server)

    message = build_message()
    message["Bcc"] = "dustin@example.com"
    message["Resent-Date"] = "Mon, 20 Nov 2017 21:04:27 -0000"
    message["Resent-From"] = "hubert@example.com"
    message["Resent-To"] = "eliza@example.com"
    message["Resent-Cc"] = "fred@example.com"
    message["Resent-Bcc"] = "gina@example.com"

    errors, _ = await client.send_message(message)

    assert not errors
    # The envelope comes from the Resent-* block only.
    assert server.mail_args == [b"FROM:<hubert@example.com> BODY=8BITMIME"]
    assert server.rcpt_args == [
        b"TO:<eliza@example.com>",
        b"TO:<fred@example.com>",
        b"TO:<gina@example.com>",
    ]

    assert len(server.data_blocks) == 1
    data = server.data_blocks[0]
    parsed = parse_data_block(data)
    assert parsed["Resent-Bcc"] is None
    assert parsed["Bcc"] is None
    assert parsed["Resent-To"] == "eliza@example.com"
    assert parsed["To"] == "to@example.com"
    assert RESENT_BCC_HEADER_REGEX.search(data) is None
    assert BCC_HEADER_REGEX.search(data) is None
    assert b"gina@example.com" not in data
    assert b"dustin@example.com" not in data

    # The caller's message object keeps both bcc fields.
    assert str(message["Resent-Bcc"]) == "gina@example.com"
    assert str(message["Bcc"]) == "dustin@example.com"


async def test_ambiguous_resent_blocks_raise_before_any_command() -> None:
    server = InMemorySMTPServer()
    client = make_connected_client(server)

    message = build_message()
    message["Resent-Date"] = "Mon, 20 Nov 2017 21:04:27 -0000"
    message["Resent-From"] = "hubert@example.com"
    message["Resent-To"] = "eliza@example.com"
    message["Resent-Date"] = "Tue, 21 Nov 2017 21:04:27 -0000"

    with pytest.raises(ValueError, match="Resent"):
        await client.send_message(message)

    # The failure happens before anything is sent on the wire.
    assert server.commands == []
    assert server.data_blocks == []


async def test_non_ascii_address_fails_without_smtputf8() -> None:
    server = InMemorySMTPServer(extensions=["8BITMIME"])
    client = make_connected_client(server)

    message = build_message()
    del message["To"]
    message["To"] = "用户@example.com"

    with pytest.raises(SMTPNotSupported, match="SMTPUTF8"):
        await client.send_message(message)

    # Only EHLO was sent: the unsupported address was not silently
    # rewritten into some other recipient on the wire.
    assert server.command_verbs == ["EHLO"]
    assert server.mail_args == []
    assert server.rcpt_args == []
    assert server.data_blocks == []


async def test_non_ascii_address_uses_smtputf8_when_supported() -> None:
    server = InMemorySMTPServer(extensions=["8BITMIME", "SMTPUTF8"])
    client = make_connected_client(server)

    message = build_message()
    del message["To"]
    message["To"] = "用户@example.com"

    errors, _ = await client.send_message(message)

    assert not errors
    assert server.mail_args == [b"FROM:<sender@example.com> SMTPUTF8 BODY=8BITMIME"]
    # The address goes on the wire as UTF-8, unchanged.
    assert server.rcpt_args == ["TO:<用户@example.com>".encode("utf-8")]

    assert len(server.data_blocks) == 1
    data = server.data_blocks[0]
    assert "用户@example.com".encode("utf-8") in data
    parsed = parse_data_block(data, policy=email.policy.SMTPUTF8)
    assert parsed["To"].addresses[0].addr_spec == "用户@example.com"


async def test_missing_8bitmime_falls_back_to_7bit_encoding() -> None:
    server = InMemorySMTPServer(extensions=[])
    client = make_connected_client(server)

    message = build_message()
    message.set_content("Grüße", cte="8bit")

    errors, _ = await client.send_message(message)

    assert not errors
    # No BODY=8BITMIME option is claimed when the server lacks 8BITMIME.
    assert server.mail_args == [b"FROM:<sender@example.com>"]
    assert server.rcpt_args == [b"TO:<to@example.com>"]

    assert len(server.data_blocks) == 1
    data = server.data_blocks[0]
    # The 8bit content is downgraded to a 7bit-safe encoding, not sent raw.
    assert data.isascii()
    parsed = parse_data_block(data)
    assert parsed.get_content() == "Grüße\n"


async def test_explicit_envelope_takes_precedence_over_headers() -> None:
    server = InMemorySMTPServer()
    client = make_connected_client(server)

    message = build_message()
    del message["From"]
    del message["To"]
    message["From"] = "header-sender@example.com"
    message["To"] = "header-to@example.com"
    message["Bcc"] = "header-bcc@example.com"

    errors, _ = await client.send_message(
        message,
        sender="envelope@example.com",
        recipients=["env-rcpt@example.com"],
    )

    assert not errors
    assert server.mail_args == [b"FROM:<envelope@example.com> BODY=8BITMIME"]
    # Only the explicit recipient is used; header To/Bcc never reach RCPT.
    assert server.rcpt_args == [b"TO:<env-rcpt@example.com>"]

    assert len(server.data_blocks) == 1
    data = server.data_blocks[0]
    parsed = parse_data_block(data)
    # Headers themselves are not rewritten, but Bcc is still stripped.
    assert parsed["From"] == "header-sender@example.com"
    assert parsed["To"] == "header-to@example.com"
    assert parsed["Bcc"] is None
    assert b"header-bcc@example.com" not in data

    assert str(message["Bcc"]) == "header-bcc@example.com"


async def test_same_message_sent_twice_keeps_fields_and_never_leaks_bcc() -> None:
    server = InMemorySMTPServer()
    client = make_connected_client(server)

    message = build_message()
    message["Bcc"] = "hidden@example.com"

    first_errors, _ = await client.send_message(message)
    second_errors, _ = await client.send_message(message)

    assert not first_errors
    assert not second_errors
    # Both transactions put the bcc address into RCPT.
    assert server.rcpt_args == [
        b"TO:<to@example.com>",
        b"TO:<hidden@example.com>",
        b"TO:<to@example.com>",
        b"TO:<hidden@example.com>",
    ]

    # Neither wire copy carries the Bcc header.
    assert len(server.data_blocks) == 2
    for data in server.data_blocks:
        assert parse_data_block(data)["Bcc"] is None
        assert BCC_HEADER_REGEX.search(data) is None
        assert b"hidden@example.com" not in data

    # The original object is untouched after both sends.
    assert str(message["Bcc"]) == "hidden@example.com"
