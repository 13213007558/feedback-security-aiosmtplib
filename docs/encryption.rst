.. _connection-types:

TLS, SSL & STARTTLS
===================

aiosmtplib supports the three main types of encryption used by SMTP servers:

1. Plaintext (default port 25). The connection is entirely unencrypted.
   Most authentication methods will not be supported by servers
   when using an unencrypted connection. Best suited for connecting to a
   server running locally or for testing.
2. TLS/SSL encrypted (default port 465). In this case the TLS handshake
   occurs when the connection is established, and all traffic is encrypted.
   This type of connection should generally be used where available.
3. STARTTLS (default port 587). When using STARTTLS, an initial unencrypted
   connection is made, EHLO/HELO greetings are exchanged, and the connection
   is upgraded in place once the client requests it by sending the STARTTLS
   command. Most servers require an upgrade before allowing AUTH commands.

.. tip:: By default, if aiosmtplib will connect in plaintext and upgrade the
   connetion using STARTTLS if the server supports it. If you want to opt out
   of upgrades even if the server supports them, pass a ``start_tls`` value of
   ``False``.

Requiring encryption for authentication
---------------------------------------

Opportunistic STARTTLS (the default) is fine for plain message delivery, but
it does not guarantee anything about the connection state by the time
credentials are sent: the server may not advertise STARTTLS, the upgrade may
fail, or the connection may have been re-established in plaintext.

Pass ``require_tls_for_auth=True`` to make the client check the actual
transport state before every authentication attempt (including automatic
login on connect), and refuse to send credentials over an unencrypted
connection. To merely *try* to upgrade, keep the defaults:

.. code-block:: python

    # Try to upgrade, but still authenticate if the connection stays plaintext
    smtp_client = aiosmtplib.SMTP(hostname="smtp.example.com", port=587)

To *require* encryption before any AUTH command is sent:

.. code-block:: python

    # Never send credentials unless the connection is encrypted
    smtp_client = aiosmtplib.SMTP(
        hostname="smtp.example.com",
        port=587,
        start_tls=True,
        require_tls_for_auth=True,
    )

With ``require_tls_for_auth=True``, :meth:`SMTP.login` and the individual
``auth_*`` methods raise :exc:`SMTPNotSupported` if the connection is not
encrypted, and no credential bytes are written to the transport. The default
value of ``False`` preserves the previous behaviour of allowing plaintext
authentication.
