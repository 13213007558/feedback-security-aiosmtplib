"""
Tests for session occupancy: the per-instance lock that keeps concurrent
operations from interleaving commands on a shared SMTP instance.

These tests use an in-memory stand-in for SMTPProtocol, so command
interleaving can be controlled precisely with event barriers, and every
command can be attributed to its response.
"""

import asyncio
from typing import Any

import pytest

from aiosmtplib import SMTP, SMTPResponse, SMTPStatus
from aiosmtplib.auth import auth_login_encode
from aiosmtplib.errors import SMTPResponseException, SMTPServerDisconnected


EHLO_MESSAGE = "fake.local\nAUTH LOGIN\nSIZE 1000"
COMMAND_VERBS = {
    b"EHLO",
    b"HELO",
    b"MAIL",
    b"RCPT",
    b"DATA",
    b"RSET",
    b"NOOP",
    b"QUIT",
    b"AUTH",
    b"VRFY",
    b"EXPN",
    b"HELP",
}


class FakeTransport:
    def __init__(self) -> None:
        self._closing = False

    def is_closing(self) -> bool:
        return self._closing

    def close(self) -> None:
        self._closing = True

    def get_extra_info(self, key: str) -> None:
        return None


class FakeSMTPProtocol:
    """
    A minimal in-memory SMTP server. Logs every command and the response
    given, so tests can verify ordering and attribution.
    """

    def __init__(self) -> None:
        self.is_connected = True
        self.log: list[tuple[str, Any]] = []
        self.barriers: dict[bytes, tuple[asyncio.Event, asyncio.Event]] = {}
        self.error_codes: dict[bytes, int] = {}
        self.disconnect_on: set[bytes] = set()

    def barrier(self, command: bytes) -> tuple[asyncio.Event, asyncio.Event]:
        entered = asyncio.Event()
        release = asyncio.Event()
        self.barriers[command] = (entered, release)
        return entered, release

    async def _handle(self, args: tuple[bytes, ...]) -> SMTPResponse:
        if not self.is_connected:
            raise SMTPServerDisconnected("Connection lost")

        command = args[0]
        self.log.append(("command", args))

        if command in self.barriers:
            entered, release = self.barriers[command]
            entered.set()
            await release.wait()

        if command in self.disconnect_on:
            self.is_connected = False
            self.log.append(("disconnect",))
            raise SMTPServerDisconnected("Connection lost")

        if command in self.error_codes:
            code = self.error_codes[command]
        elif command not in COMMAND_VERBS:
            # A raw line (e.g. an AUTH continuation) rather than a verb
            code = SMTPStatus.auth_successful
        elif command == b"EHLO":
            self.log.append(("response", SMTPStatus.completed))
            return SMTPResponse(SMTPStatus.completed, EHLO_MESSAGE)
        elif command == b"AUTH":
            code = SMTPStatus.auth_continue
        elif command == b"QUIT":
            code = SMTPStatus.closing
        else:
            code = SMTPStatus.completed

        self.log.append(("response", code))
        return SMTPResponse(code, "OK")

    async def execute_command(self, *args: bytes, timeout: Any = None) -> SMTPResponse:
        return await self._handle(args)

    async def execute_data_command(
        self, message: bytes, timeout: Any = None
    ) -> SMTPResponse:
        if not self.is_connected:
            raise SMTPServerDisconnected("Connection lost")

        self.log.append(("command", (b"DATA",)))
        if b"DATA" in self.barriers:
            entered, release = self.barriers[b"DATA"]
            entered.set()
            await release.wait()

        if b"DATA" in self.disconnect_on:
            self.is_connected = False
            self.log.append(("disconnect",))
            raise SMTPServerDisconnected("Connection lost")

        self.log.append(("data", message))
        self.log.append(("response", SMTPStatus.completed))
        return SMTPResponse(SMTPStatus.completed, "queued")


def make_client(protocol: FakeSMTPProtocol) -> SMTP:
    client = SMTP(timeout=5.0, local_hostname="fake.local")
    client.protocol = protocol  # type: ignore[assignment]
    client.transport = FakeTransport()  # type: ignore[assignment]
    return client


def logged_commands(protocol: FakeSMTPProtocol) -> list[tuple[bytes, ...]]:
    return [entry[1] for entry in protocol.log if entry[0] == "command"]


def assert_commands_pair_with_responses(
    protocol: FakeSMTPProtocol,
) -> None:
    filtered = [entry for entry in protocol.log if entry[0] != "data"]
    for index in range(0, len(filtered), 2):
        assert filtered[index][0] == "command"
        assert filtered[index + 1][0] == "response"


def envelope_blocks(
    protocol: FakeSMTPProtocol,
) -> list[tuple[tuple[bytes, ...], list[tuple[bytes, ...]], bytes]]:
    """
    Collect (MAIL, [RCPT...], body) per envelope, asserting that no
    foreign command was interleaved into any envelope.
    """
    blocks = []
    mail: tuple[bytes, ...] | None = None
    rcpts: list[tuple[bytes, ...]] = []
    bodies = [entry[1] for entry in protocol.log if entry[0] == "data"]
    for command in logged_commands(protocol):
        if command[0] == b"MAIL":
            assert mail is None, "MAIL interleaved into another envelope"
            mail = command
            rcpts = []
        elif command[0] == b"RCPT":
            assert mail is not None, "RCPT outside an envelope"
            rcpts.append(command)
        elif command[0] == b"DATA":
            assert mail is not None, "DATA outside an envelope"
            blocks.append((mail, rcpts, bodies[len(blocks)]))
            mail = None
        else:
            assert mail is None, f"{command[0]!r} interleaved into an envelope"
    return blocks


async def test_concurrent_operations_do_not_interleave() -> None:
    """
    Two sendmails, a NOOP and a login on one shared instance: while one
    operation holds the session, no other operation's commands may reach
    the wire, and each message body goes only to its own recipients.
    """
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    await client.ehlo()

    mail_entered, mail_release = protocol.barrier(b"MAIL")

    task_a = asyncio.create_task(
        client.sendmail("alice@example.com", ["alice-rcpt@example.com"], "body A")
    )
    await mail_entered.wait()

    task_b = asyncio.create_task(
        client.sendmail("bob@example.com", ["bob-rcpt@example.com"], "body B")
    )
    task_c = asyncio.create_task(client.noop())
    task_d = asyncio.create_task(client.login("user", "pass"))
    await asyncio.sleep(0.1)

    # A holds the session mid-MAIL; nothing else may reach the wire
    assert logged_commands(protocol) == [
        (b"EHLO", b"fake.local"),
        (b"MAIL", b"FROM:<alice@example.com>", b"size=8"),
    ]

    mail_release.set()
    await asyncio.gather(task_a, task_b, task_c, task_d)

    assert_commands_pair_with_responses(protocol)

    blocks = envelope_blocks(protocol)
    assert len(blocks) == 2
    bodies = {block[2] for block in blocks}
    assert bodies == {b"body A", b"body B"}
    for mail, rcpts, body in blocks:
        if b"alice@example.com" in mail[1]:
            assert rcpts == [(b"RCPT", b"TO:<alice-rcpt@example.com>")]
            assert body == b"body A"
        else:
            assert b"bob@example.com" in mail[1]
            assert rcpts == [(b"RCPT", b"TO:<bob-rcpt@example.com>")]
            assert body == b"body B"

    commands = logged_commands(protocol)
    assert commands.count((b"NOOP",)) == 1

    # The multi-round AUTH LOGIN was not interrupted either
    encoded_username, encoded_password = auth_login_encode("user", "pass")
    auth_index = commands.index((b"AUTH", b"LOGIN", encoded_username))
    assert commands[auth_index + 1] == (encoded_password,)


async def test_explicit_transaction_blocks_interleaving() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    rcpt_entered, rcpt_release = protocol.barrier(b"RCPT")

    async def send_lowlevel() -> None:
        async with client.transaction():
            await client.mail("alice@example.com")
            await client.rcpt("alice-rcpt@example.com")
            await client.data("body A")

    send_task = asyncio.create_task(send_lowlevel())
    await rcpt_entered.wait()

    noop_task = asyncio.create_task(client.noop())
    await asyncio.sleep(0.1)

    # The NOOP is still queued behind the explicit transaction
    assert (b"NOOP",) not in logged_commands(protocol)

    rcpt_release.set()
    await send_task
    await noop_task

    commands = logged_commands(protocol)
    assert commands.index((b"NOOP",)) > commands.index((b"DATA",))


async def test_nested_transactions_do_not_deadlock() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)

    async with client.transaction():
        async with client.transaction():
            await client.noop()
        # High-level operations nest transparently
        await client.sendmail("alice@example.com", ["bob@example.com"], "body")

    assert not client._get_session_lock().locked()
    response = await client.noop()
    assert response.code == SMTPStatus.completed


async def test_nested_transaction_failure_cleans_up_once() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    await client.ehlo()
    protocol.error_codes[b"RCPT"] = 550

    with pytest.raises(SMTPResponseException):
        async with client.transaction():
            async with client.transaction():
                await client.mail("alice@example.com")
                await client.rcpt("bob@example.com")

    # Only the outermost transaction resets the envelope
    assert logged_commands(protocol).count((b"RSET",)) == 1
    assert client.is_connected


async def test_transaction_resets_envelope_on_response_error() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    await client.ehlo()
    protocol.error_codes[b"RCPT"] = 550

    with pytest.raises(SMTPResponseException):
        async with client.transaction():
            await client.mail("alice@example.com")
            await client.rcpt("bob@example.com")

    assert logged_commands(protocol)[-1] == (b"RSET",)
    assert client.is_connected
    response = await client.noop()
    assert response.code == SMTPStatus.completed


async def test_transaction_closes_connection_when_reset_fails() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    await client.ehlo()
    protocol.error_codes[b"RCPT"] = 550
    protocol.error_codes[b"RSET"] = 500

    with pytest.raises(SMTPResponseException):
        async with client.transaction():
            await client.mail("alice@example.com")
            await client.rcpt("bob@example.com")

    # The envelope could not be reset, so the session is not reused
    assert not client.is_connected
    assert not client._get_session_lock().locked()


async def test_session_lock_release_by_other_task_raises() -> None:
    client = make_client(FakeSMTPProtocol())
    lock = client._get_session_lock()
    await lock.acquire()

    async def release() -> None:
        lock.release()

    with pytest.raises(RuntimeError):
        await asyncio.create_task(release())

    lock.release()
    assert not lock.locked()


async def test_transaction_cancel_closes_connection() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)

    with pytest.raises(asyncio.CancelledError):
        async with client.transaction():
            await client.noop()
            raise asyncio.CancelledError

    assert not client.is_connected
    assert not client._get_session_lock().locked()


async def test_cancelled_waiter_does_not_lose_the_lock() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    noop_entered, noop_release = protocol.barrier(b"NOOP")

    holder_task = asyncio.create_task(client.noop())
    await noop_entered.wait()

    cancelled_task = asyncio.create_task(client.noop())
    queued_task = asyncio.create_task(client.noop())
    await asyncio.sleep(0.1)

    cancelled_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_task

    noop_release.set()
    assert (await holder_task).code == SMTPStatus.completed
    # Cancelling a waiter must not lose the lock for those behind it
    assert (await queued_task).code == SMTPStatus.completed

    assert client.is_connected
    assert not client._get_session_lock().locked()
    assert (await client.noop()).code == SMTPStatus.completed


async def test_failed_connection_is_not_reused_by_next_waiter() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    data_entered, data_release = protocol.barrier(b"DATA")
    protocol.disconnect_on.add(b"DATA")

    send_task = asyncio.create_task(
        client.sendmail("alice@example.com", ["bob@example.com"], "body")
    )
    await data_entered.wait()

    noop_task = asyncio.create_task(client.noop())
    await asyncio.sleep(0.1)

    data_release.set()
    with pytest.raises(SMTPServerDisconnected):
        await send_task

    # The queued operation gets a closed connection, not a broken session
    with pytest.raises(SMTPServerDisconnected):
        await noop_task
    assert not client.is_connected
    assert not client._get_session_lock().locked()


async def test_quit_waits_for_current_operation_and_disconnects_waiters() -> None:
    protocol = FakeSMTPProtocol()
    client = make_client(protocol)
    noop_entered, noop_release = protocol.barrier(b"NOOP")

    noop_task = asyncio.create_task(client.noop())
    await noop_entered.wait()

    quit_task = asyncio.create_task(client.quit())
    queued_task = asyncio.create_task(client.noop())
    await asyncio.sleep(0.1)

    noop_release.set()
    assert (await noop_task).code == SMTPStatus.completed
    assert (await quit_task).code == SMTPStatus.closing

    with pytest.raises(SMTPServerDisconnected):
        await queued_task
    assert not client.is_connected
    assert logged_commands(protocol) == [
        (b"EHLO", b"fake.local"),
        (b"NOOP",),
        (b"QUIT",),
    ]


async def test_session_lock_is_per_instance() -> None:
    """
    Two instances must not serialize each other; if a global lock were
    used, the second client could never reach DATA while the first waits.
    """
    protocol_a = FakeSMTPProtocol()
    protocol_b = FakeSMTPProtocol()
    client_a = make_client(protocol_a)
    client_b = make_client(protocol_b)

    data_a_entered, data_a_release = protocol_a.barrier(b"DATA")
    data_b_entered, data_b_release = protocol_b.barrier(b"DATA")

    task_a = asyncio.create_task(
        client_a.sendmail("alice@example.com", ["amy@example.com"], "body A")
    )
    task_b = asyncio.create_task(
        client_b.sendmail("bob@example.com", ["ben@example.com"], "body B")
    )

    await asyncio.wait_for(
        asyncio.gather(data_a_entered.wait(), data_b_entered.wait()), timeout=1
    )
    data_a_release.set()
    data_b_release.set()
    await asyncio.gather(task_a, task_b)
