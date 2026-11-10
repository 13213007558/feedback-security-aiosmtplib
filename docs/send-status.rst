.. py:currentmodule:: aiosmtplib

Transaction State and Retrying After Failures
==============================================

A failure while sending mail does not always mean the message was not sent.
If the connection drops after the message data (including its end-of-data
marker) has been handed to the server, the server may already have accepted
and delivered the message even though no final reply was observed. Treating
every timeout or disconnect as "nothing was sent" and resending blindly can
therefore produce duplicate messages.

aiosmtplib never retries a mail transaction on its own. Instead, every
:meth:`SMTP.sendmail` call records how far the transaction got in an
:class:`SMTPSendResult`, so the application can decide what to do.

Reading the Result
------------------

The most recent result is available on the client as
:attr:`SMTP.last_send_result`, and the same object is attached to whatever
exception the call raises (including :class:`asyncio.CancelledError`) as its
``send_result`` attribute. The top-level :func:`send` coroutine attaches it
to the exception as well, since its internal client is not reusable.

.. testcode::

    import aiosmtplib
    from aiosmtplib import SMTPSendState

    async def send_with_status(client, sender, recipients, message):
        try:
            errors, reply = await client.sendmail(sender, recipients, message)
        except Exception as exc:
            result = getattr(exc, "send_result", None)
            if result is None:
                raise
            return result.state
        return client.last_send_result.state

The Three Confirmation Groups
------------------------------

:class:`SMTPSendState` values fall into three groups with different retry
implications:

1. **Not submitted.** :attr:`SMTPSendState.not_started`,
   :attr:`SMTPSendState.envelope_open` and
   :attr:`SMTPSendState.data_interrupted`. The end-of-data marker was never
   handed to the transport, so the server cannot have accepted the message.
   It is safe to resend, preferably on a fresh connection.

2. **Definitive reply received.** :attr:`SMTPSendState.accepted` and
   :attr:`SMTPSendState.rejected`. The server's final answer was observed.
   An accepted transaction is complete; a rejected one
   (:attr:`SMTPSendResult.response` holds the refusal) may be retried if the
   application considers that specific failure transient.

3. **Outcome unknown.** :attr:`SMTPSendState.unknown`. The end-of-data marker
   was handed to the transport, but the connection dropped, timed out or was
   cancelled before the final reply. Delivery may or may not have happened.
   The helper properties :attr:`SMTPSendState.submitted` (true for
   ``accepted``, ``rejected`` and ``unknown``) and
   :attr:`SMTPSendState.definitive` (true for ``accepted`` and ``rejected``)
   make these checks explicit.

Per-Recipient Status
--------------------

A RCPT TO acknowledgement is an envelope-level acceptance by this server, not
proof of final delivery. The result keeps each recipient separate:

- :attr:`SMTPSendResult.accepted_recipients` lists recipients whose RCPT TO
  was accepted (250/251).
- :attr:`SMTPSendResult.refused_recipients` maps refused recipients to the
  server's :class:`SMTPResponse`.

The successful return value of :meth:`SMTP.sendmail` is unchanged for
backwards compatibility: a ``(refused_dict, message)`` tuple. The result
object stores envelope addresses and server replies only; it never retains
the message body.

Deciding What To Do
-------------------

.. testcode::

    async def send_idempotently(client, sender, recipients, message):
        try:
            return await client.sendmail(sender, recipients, message)
        except (aiosmtplib.SMTPServerDisconnected, aiosmtplib.SMTPTimeoutError) as exc:
            result = exc.send_result
            if result is None or not result.state.submitted:
                # The message never reached the point of no return: a fresh
                # connection can safely try again.
                new_client = aiosmtplib.SMTP(hostname=client.hostname, port=client.port)
                async with new_client:
                    return await new_client.sendmail(sender, recipients, message)

            if result.state is aiosmtplib.SMTPSendState.unknown:
                # The server may already have delivered it. Do not resend
                # blindly: queue for manual review, or retry only if the
                # downstream handling is idempotent (e.g. the server
                # deduplicates on a stable Message-ID header).
                await queue_for_reconciliation(result)
            raise

Cancellation behaves exactly like ordinary :mod:`asyncio` cancellation: the
await raises :class:`asyncio.CancelledError`, the connection is closed
because an in-flight reply can no longer be paired safely, and the result on
the exception reports the confirmation boundary that had been reached.
