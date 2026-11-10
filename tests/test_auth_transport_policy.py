"""
Auth transport policy (require_tls_for_auth) tests.

These tests use in-memory protocol/transport stand-ins that record outbound
bytes, so we can prove that no credential bytes are written when the
connection is not encrypted.
"""

import ssl
from typing import Any

import pytest

from aiosmtplib import SMTP, SMTPNotSupported, SMTPResponse, SMTPStatus
from aiosmtplib.errors import SMTPResponseException


EHLO_WITH_STARTTLS = "example.com\nSTARTTLS\nAUTH PLAIN LOGIN CRAM-MD5"
EHLO_WITHOUT_STARTTLS = "example.com\nAUTH PLAIN LOGIN CRAM-MD5"

PASSWORD = "s3cr3t-p@ssword"


class RecordingTransport:
    """
    Minimal transport stand-in; only what SMTP touches.
    """

    def __init__(self, tls_context: ssl.SSLContext | None = None) -> None:
        self.tls_context = tls_context
        self.closed = False

    def get_extra_info(self, key: str) -> Any:
        if key == "sslcontext":
            return self.tls_context
        return None

    def is_closing(self) -> bool:
        return False

    def close(self) -> None:
        self.closed = True


class RecordingProtocol:
    """
    In-memory stand-in for SMTPProtocol that records all outbound bytes.
    """

    def __init__(
        self,
        ehlo_response: str = EHLO_WITH_STARTTLS,
        start_tls_error: Exception | None = None,
    ) -> None:
        self.is_connected = True
        self.ehlo_response = ehlo_response
        self.start_tls_error = start_tls_error
        self.written = bytearray()
        self.transport: RecordingTransport | None = None

    async def execute_command(self, *args: bytes, timeout: float | None = None):
        self.written += b" ".join(args) + b"\r\n"

        if args[0].upper() == b"EHLO":
            return SMTPResponse(SMTPStatus.completed, self.ehlo_response)
        if len(args) > 1 and args[0].upper() == b"AUTH":
            mechanism = args[1].upper()
            if mechanism in (b"LOGIN", b"CRAM-MD5"):
                return SMTPResponse(SMTPStatus.auth_continue, "VXNlcm5hbWU6")
            return SMTPResponse(SMTPStatus.auth_successful, "OK")
        # Continuation command (LOGIN password, CRAM-MD5 verification)
        return SMTPResponse(SMTPStatus.auth_successful, "OK")

    async def start_tls(
        self,
        tls_context: ssl.SSLContext,
        server_hostname: str | None = None,
        timeout: float | None = None,
    ) -> SMTPResponse:
        if self.start_tls_error is not None:
            raise self.start_tls_error
        assert self.transport is not None
        self.transport.tls_context = tls_context
        return SMTPResponse(SMTPStatus.ready, "Ready to start TLS")


def make_client(
    *,
    tls: bool = False,
    ehlo_response: str = EHLO_WITH_STARTTLS,
    start_tls_error: Exception | None = None,
    **kwargs: Any,
) -> tuple[SMTP, RecordingProtocol]:
    kwargs.setdefault("require_tls_for_auth", True)
    client = SMTP(hostname="example.com", local_hostname="test.local", **kwargs)
    attach_connection(
        client, tls=tls, ehlo_response=ehlo_response, start_tls_error=start_tls_error
    )
    return client, client.protocol  # type: ignore[return-value]


def attach_connection(
    client: SMTP,
    *,
    tls: bool,
    ehlo_response: str = EHLO_WITH_STARTTLS,
    start_tls_error: Exception | None = None,
) -> RecordingProtocol:
    protocol = RecordingProtocol(
        ehlo_response=ehlo_response, start_tls_error=start_tls_error
    )
    tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT) if tls else None
    transport = RecordingTransport(tls_context=tls_context)
    protocol.transport = transport
    client.protocol = protocol  # type: ignore[assignment]
    client.transport = transport
    return protocol


async def test_login_allowed_with_implicit_tls() -> None:
    client, protocol = make_client(tls=True)

    response = await client.login("user", PASSWORD)

    assert response.code == SMTPStatus.auth_successful
    assert b"AUTH" in protocol.written


async def test_login_allowed_after_successful_starttls() -> None:
    client, protocol = make_client(tls=False)

    await client.starttls()
    response = await client.login("user", PASSWORD)

    assert response.code == SMTPStatus.auth_successful
    assert b"AUTH" in protocol.written


async def test_login_blocked_when_starttls_not_advertised() -> None:
    client, protocol = make_client(ehlo_response=EHLO_WITHOUT_STARTTLS)

    with pytest.raises(SMTPNotSupported) as excinfo:
        await client.login("user", PASSWORD)

    assert "STARTTLS" in str(excinfo.value)
    assert PASSWORD not in str(excinfo.value)
    assert b"AUTH" not in protocol.written
    assert PASSWORD.encode() not in protocol.written


async def test_login_blocked_when_upgrade_not_performed() -> None:
    client, protocol = make_client()

    with pytest.raises(SMTPNotSupported) as excinfo:
        await client.login("user", PASSWORD)

    assert "not encrypted" in str(excinfo.value)
    assert PASSWORD not in str(excinfo.value)
    assert b"AUTH" not in protocol.written


@pytest.mark.parametrize(
    "start_tls_error",
    [
        SMTPResponseException(SMTPStatus.tls_not_available, "TLS not available"),
        ssl.SSLCertVerificationError("certificate verify failed"),
    ],
    ids=["upgrade_refused", "cert_verification_failed"],
)
async def test_login_blocked_after_starttls_failure(
    start_tls_error: Exception,
) -> None:
    client, protocol = make_client(start_tls_error=start_tls_error)

    with pytest.raises(type(start_tls_error)):
        await client.starttls()

    with pytest.raises(SMTPNotSupported):
        await client.login("user", PASSWORD)

    assert b"AUTH" not in protocol.written


@pytest.mark.parametrize(
    "method_name,args",
    [
        ("auth_crammd5", ("user", PASSWORD)),
        ("auth_plain", ("user", PASSWORD)),
        ("auth_login", ("user", PASSWORD)),
        ("auth_xoauth2", ("user", "oauth-token")),
    ],
)
async def test_direct_auth_method_blocked_without_tls(
    method_name: str, args: tuple[str, str]
) -> None:
    client, protocol = make_client()

    with pytest.raises(SMTPNotSupported) as excinfo:
        await getattr(client, method_name)(*args)

    assert PASSWORD not in str(excinfo.value)
    assert b"AUTH" not in protocol.written
    assert PASSWORD.encode() not in protocol.written


async def test_direct_auth_method_allowed_with_tls() -> None:
    client, protocol = make_client(tls=True)

    response = await client.auth_plain("user", PASSWORD)

    assert response.code == SMTPStatus.auth_successful
    assert b"AUTH" in protocol.written


async def test_reconnect_does_not_inherit_tls_state() -> None:
    client, protocol = make_client(tls=True)
    await client.login("user", PASSWORD)
    assert b"AUTH" in protocol.written

    # Simulate a reconnect: the encrypted connection is gone, and the new
    # connection has not been upgraded yet.
    client.close()
    new_protocol = attach_connection(client, tls=False)

    with pytest.raises(SMTPNotSupported):
        await client.login("user", PASSWORD)

    assert b"AUTH" not in new_protocol.written


async def test_auto_login_on_connect_blocked_without_tls() -> None:
    client, protocol = make_client(username="user", password=PASSWORD)

    with pytest.raises(SMTPNotSupported):
        await client._maybe_login_on_connect()

    assert b"AUTH" not in protocol.written


async def test_auto_login_on_connect_allowed_with_tls() -> None:
    client, protocol = make_client(tls=True, username="user", password=PASSWORD)

    await client._maybe_login_on_connect()

    assert b"AUTH" in protocol.written


async def test_plaintext_login_allowed_when_policy_disabled() -> None:
    client, protocol = make_client(require_tls_for_auth=False)

    response = await client.login("user", PASSWORD)

    assert response.code == SMTPStatus.auth_successful
    assert b"AUTH" in protocol.written


async def test_require_tls_for_auth_setting_update() -> None:
    client = SMTP()
    assert client.require_tls_for_auth is False

    client._update_settings_from_kwargs(require_tls_for_auth=True)
    assert client.require_tls_for_auth is True

    client._update_settings_from_kwargs()
    assert client.require_tls_for_auth is True
