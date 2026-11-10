"""
Tests for the transaction-state reporting added around ``sendmail``.

Failure injection uses an in-process, scripted SMTP server connected over a
``socket.socketpair`` (no real network ports), so a test can fail the
connection or stall at a precise SMTP boundary: before MAIL, partway through
RCPT, before/after the DATA end marker, or at the final 250.
"""

import asyncio
import socket
from typing import Awaitable, Callable

import pytest

from aiosmtplib import (
    SMTP,
    SMTPDataError,
    SMTPSendState,
    SMTPServerDisconnected,
    SMTPTimeoutError,
)
from aiosmtplib.protocol import DATA_CHUNK_SIZE, SMTPProtocol


SENDER = "sender@example.com"
RECIPIENTS = ("one@example.com", "two@example.com")
MESSAGE = b"Subject: testing\r\n\r\nHello world\r\n"


class ScriptedSMTPServer(asyncio.Protocol):
    """
    Minimal SMTP server that drops or stalls at a chosen boundary.

    ``fail_at`` names the boundary at which ``fail_mode`` is triggered:

    - ``mail``: when MAIL FROM arrives
    - ``rcpt``: when the *second* RCPT TO arrives (the first is accepted)
    - ``rcpt_stall``: withhold the reply to the second RCPT TO
    - ``data_command``: when DATA arrives (before the 354 reply)
    - ``data_upload``: right after the 354 reply, before reading the body
    - ``after_dot``: once the end-of-data marker is received
    - ``final_reply``: after a normal 250 would be produced (the reply is
      withheld and the connection dropped instead)

    ``fail_mode`` is ``disconnect`` (close the transport) or ``stall``
    (stop responding, so the client times out).
    """

    def __init__(
        self,
        fail_at: str | None = None,
        fail_mode: str = "disconnect",
        *,
        refuse_rcpt: bool = False,
        refuse_all_rcpt: bool = False,
        reject_data: bool = False,
    ) -> None:
        self.fail_at = fail_at
        self.fail_mode = fail_mode
        self.refuse_rcpt = refuse_rcpt
        self.refuse_all_rcpt = refuse_all_rcpt
        self.reject_data = reject_data
        self.transport: asyncio.BaseTransport | None = None
        self._buffer = bytearray()
        self.rcpt_count = 0
        self.received_envelope_rcpts: list[str] = []
        self.got_end_marker = False
        self.delivered = False
        self.commands: list[str] = []

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self.transport.write(b"220 scripted ready\r\n")

    def connection_lost(self, exc: Exception | None) -> None:
        self.transport = None

    def data_received(self, data: bytes) -> None:
        self._buffer.extend(data)
        while b"\r\n" in self._buffer:
            line, _, rest = self._buffer.partition(b"\r\n")
            self._buffer = bytearray(rest)
            self._handle_line(bytes(line))
            if self.transport is None:
                return

    def _handle_line(self, line: bytes) -> None:
        command = line.split(b" ", 1)[0].upper()
        self.commands.append(command.decode("ascii"))

        if command == b"EHLO" or command == b"HELO":
            self._reply(b"250-localhost\r\n250 SIZE 1000000\r\n")
        elif command == b"MAIL":
            if self.fail_at == "mail":
                self._fail()
                return
            self._reply(b"250 OK\r\n")
        elif command == b"RCPT":
            self.rcpt_count += 1
            address = line.split(b":", 1)[1].strip(b"<>").decode("ascii")
            self.received_envelope_rcpts.append(address)
            if self.fail_at == "rcpt" and self.rcpt_count == 2:
                self._fail()
                return
            if self.fail_at == "rcpt_stall" and self.rcpt_count == 2:
                # Withhold the reply; the client parks mid-envelope.
                return
            if self.refuse_all_rcpt or (self.refuse_rcpt and self.rcpt_count == 2):
                self._reply(b"550 mailbox unavailable\r\n")
            else:
                self._reply(b"250 OK\r\n")
        elif command == b"DATA":
            if self.fail_at == "data_command":
                self._fail()
                return
            self._reply(b"354 End data with <CR><LF>.<CR><LF>\r\n")
            if self.fail_at == "data_upload":
                # Stop processing; the client's body write stalls/times out.
                self._fail()
                return
        elif line == b".":
            self.got_end_marker = True
            if self.fail_at == "after_dot":
                self._fail()
                return
            if self.reject_data:
                self._reply(b"451 try again later\r\n")
            else:
                self.delivered = True
                if self.fail_at == "final_reply":
                    self._fail()
                    return
                self._reply(b"250 message accepted\r\n")
        elif command == b"RSET":
            self._reply(b"250 OK\r\n")
        elif command == b"QUIT":
            self._reply(b"221 bye\r\n")
            if self.transport is not None:
                self.transport.close()
        elif command == b"NOOP":
            self._reply(b"250 OK\r\n")

    def _reply(self, data: bytes) -> None:
        if self.transport is not None:
            self.transport.write(data)

    def _fail(self) -> None:
        if self.fail_mode == "disconnect" and self.transport is not None:
            self.transport.close()
            self.transport = None
        # "stall": simply do nothing and keep the (paused) connection open.


@pytest.fixture
def socketpair_client() -> Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]]:
    async def _factory(
        server: ScriptedSMTPServer, *, timeout: float = 0.3
    ) -> tuple[SMTP, ScriptedSMTPServer]:
        loop = asyncio.get_running_loop()
        client_sock, server_sock = socket.socketpair()
        client_sock.setblocking(False)
        server_sock.setblocking(False)

        server_transport, _ = await loop.create_connection(
            lambda: server, sock=server_sock
        )
        client = SMTP(timeout=timeout)
        client.loop = loop
        transport, protocol = await loop.create_connection(
            lambda: SMTPProtocol(
                loop=loop, connection_lost_callback=client._on_connection_lost
            ),
            sock=client_sock,
        )
        client.protocol = protocol
        client.transport = transport
        # Consume the greeting the same way SMTP.connect does.
        await protocol.read_response(timeout=timeout)
        client._connect_lock = asyncio.Lock()

        return client, server

    return _factory


def make_server(**kwargs: object) -> ScriptedSMTPServer:
    return ScriptedSMTPServer(**kwargs)  # type: ignore[arg-type]


async def test_send_success_reports_accepted(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server()
    client, server = await socketpair_client(server)

    errors, message = await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    assert not errors
    assert message == "message accepted"
    result = client.last_send_result
    assert result is not None
    assert result.state is SMTPSendState.accepted
    assert result.state.definitive
    assert result.state.submitted
    assert result.response is not None and result.response.code == 250
    assert result.accepted_recipients == RECIPIENTS
    assert result.refused_recipients == {}
    assert result.error is None
    # The body is never retained in the result.
    assert not hasattr(result, "message_body")
    assert server.delivered


async def test_disconnect_before_mail_is_not_submitted(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="mail")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result is client.last_send_result
    # MAIL FROM was never accepted, so nothing was submitted.
    assert result.state is SMTPSendState.not_started
    assert not result.state.submitted
    assert result.accepted_recipients == ()
    assert not server.got_end_marker


async def test_disconnect_partway_through_rcpt_keeps_per_recipient_status(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="rcpt")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.envelope_open
    assert not result.state.submitted
    # The first RCPT was acknowledged, but that is not delivery.
    assert result.accepted_recipients == ("one@example.com",)
    assert result.sender == SENDER
    assert result.recipients == RECIPIENTS
    # Both RCPT commands reached the wire, but only the first got an
    # acknowledgement; acceptance is tracked per reply, not per command.
    assert server.received_envelope_rcpts == list(RECIPIENTS)
    assert not server.got_end_marker


async def test_refused_rcpt_is_recorded_not_treated_as_delivery(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(refuse_rcpt=True)
    client, server = await socketpair_client(server)

    errors, _ = await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    assert set(errors) == {"two@example.com"}
    result = client.last_send_result
    assert result.state is SMTPSendState.accepted
    assert result.accepted_recipients == ("one@example.com",)
    assert "two@example.com" in result.refused_recipients
    assert result.refused_recipients["two@example.com"].code == 550
    # Backwards-compatible success tuple is unchanged.
    assert isinstance(errors, dict)


async def test_all_rcpt_refused_keeps_not_submitted_state(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    from aiosmtplib import SMTPRecipientsRefused

    server = make_server(refuse_all_rcpt=True)
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPRecipientsRefused):
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = client.last_send_result
    assert result.state is SMTPSendState.envelope_open
    assert not result.state.submitted
    assert result.accepted_recipients == ()
    assert set(result.refused_recipients) == set(RECIPIENTS)


async def test_disconnect_on_data_command_is_not_submitted(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="data_command")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.data_interrupted
    assert not result.state.submitted
    assert not server.got_end_marker


class PausingTransport:
    """Wraps a transport and pauses writing on the first DATA body chunk."""

    def __init__(
        self, wrapped: asyncio.BaseTransport, protocol: asyncio.Protocol
    ) -> None:
        self._wrapped = wrapped
        self._protocol = protocol
        self._data_seen = False
        self._paused = False

    def write(self, data: bytes) -> None:
        cast_write(self._wrapped).write(data)
        if data == b"DATA\r\n":
            self._data_seen = True
        elif self._data_seen and not self._paused:
            # The first body chunk (which may already include the end marker)
            # stalls in the drain, so the marker is never handed off.
            self._paused = True
            self._protocol.pause_writing()

    def __getattr__(self, name: str) -> object:
        return getattr(self._wrapped, name)


def cast_write(transport: asyncio.BaseTransport) -> asyncio.WriteTransport:
    return transport  # type: ignore[return-value]


async def test_stall_during_upload_times_out_not_submitted(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="data_upload", fail_mode="stall")
    client, server = await socketpair_client(server, timeout=0.1)
    assert client.protocol is not None
    # Pause the protocol as soon as the first body chunk is written, so the
    # drain never completes and the inactivity timeout fires before the
    # end-of-data marker (which lives in the final chunk) is handed off. The
    # protocol keeps its own transport reference, so wrap both it and client.
    wrapper = PausingTransport(client.transport, client.protocol)  # type: ignore[arg-type]
    client.transport = wrapper
    client.protocol.transport = wrapper

    with pytest.raises(SMTPTimeoutError) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), b"x" * (DATA_CHUNK_SIZE * 2))

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.data_interrupted
    assert not result.state.submitted
    assert not server.got_end_marker


async def test_disconnect_after_end_marker_is_unknown(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="after_dot")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.unknown
    assert result.state.submitted
    assert not result.state.definitive
    assert server.got_end_marker
    assert not server.delivered  # the scripted server itself never answered 250


async def test_timeout_waiting_for_final_reply_is_unknown(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="after_dot", fail_mode="stall")
    client, server = await socketpair_client(server, timeout=0.1)

    with pytest.raises(SMTPTimeoutError) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.unknown
    assert result.state.submitted
    assert server.got_end_marker


async def test_disconnect_instead_of_final_reply_is_unknown(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="final_reply")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.unknown
    assert result.state.submitted
    # The server had in fact accepted and queued the message.
    assert server.delivered


async def test_definitive_rejection_after_dot_is_rejected(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(reject_data=True)
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPDataError) as exc_info:
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    result = exc_info.value.send_result
    assert result.state is SMTPSendState.rejected
    assert result.state.definitive
    assert result.state.submitted
    assert result.response is not None and result.response.code == 451
    assert isinstance(result.error, SMTPDataError)


async def test_cancellation_after_end_marker_preserves_cancel_and_state(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="after_dot", fail_mode="stall")
    client, server = await socketpair_client(server, timeout=5.0)

    task = asyncio.ensure_future(client.sendmail(SENDER, list(RECIPIENTS), MESSAGE))
    # Wait until the end marker is on the wire and only the final reply is
    # outstanding, then cancel.
    deadline = asyncio.get_running_loop().time() + 2.0
    while not server.got_end_marker and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert server.got_end_marker
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await task

    # Standard asyncio cancellation semantics: a CancelledError, nothing else.
    assert type(exc_info.value) is asyncio.CancelledError
    result = getattr(exc_info.value, "send_result", None)
    assert result is client.last_send_result
    assert result.state is SMTPSendState.unknown
    assert result.state.submitted
    # Cancellation drops the connection rather than leaving it desynced.
    assert not client.is_connected


async def test_cancellation_before_data_keeps_not_submitted_state(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="rcpt_stall")
    client, server = await socketpair_client(server, timeout=5.0)

    task = asyncio.ensure_future(client.sendmail(SENDER, list(RECIPIENTS), MESSAGE))
    # The second RCPT reply is withheld, so the task parks mid-envelope.
    deadline = asyncio.get_running_loop().time() + 2.0
    while server.rcpt_count < 2 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert server.rcpt_count == 2
    task.cancel()

    with pytest.raises(asyncio.CancelledError) as exc_info:
        await task

    result = getattr(exc_info.value, "send_result", None)
    assert result is not None
    assert not result.state.submitted
    assert result.state is SMTPSendState.envelope_open


async def test_library_does_not_resend_unknown_message(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    """
    An unknown-outcome failure must not trigger any automatic second attempt:
    the library raises with state ``unknown`` and never opens a second
    transaction on its own.
    """
    server = make_server(fail_at="after_dot")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected):
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    # Exactly one MAIL and one end-of-data marker were ever sent.
    assert server.commands.count("MAIL") == 1
    assert server.commands.count("DATA") == 1
    assert server.got_end_marker is True


async def test_send_result_survives_close(
    socketpair_client: Callable[..., Awaitable[tuple[SMTP, ScriptedSMTPServer]]],
) -> None:
    server = make_server(fail_at="after_dot")
    client, server = await socketpair_client(server)

    with pytest.raises(SMTPServerDisconnected):
        await client.sendmail(SENDER, list(RECIPIENTS), MESSAGE)

    assert client.protocol is None
    result = client.last_send_result
    assert result is not None
    assert result.state is SMTPSendState.unknown
    assert result.recipients == RECIPIENTS
