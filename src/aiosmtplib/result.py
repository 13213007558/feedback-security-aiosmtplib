"""
Result of an attempted mail transaction.

Unlike the tuple returned by :meth:`.SMTP.sendmail`, which only reports a
completed transaction, :class:`SMTPSendResult` is also populated while a
transaction is in progress, so it can be inspected after a disconnect,
timeout or cancellation to see how far the transaction got.
"""

import enum
from dataclasses import dataclass, field
from typing import cast

from .response import SMTPResponse


__all__ = ("SMTPSendState", "SMTPSendResult", "attach_send_result")


class _SendResultHolder:
    """Structural type for objects carrying a ``send_result`` attribute."""

    send_result: "SMTPSendResult | None"


def attach_send_result(error: BaseException, result: "SMTPSendResult | None") -> None:
    """
    Attach transaction progress to an arbitrary exception.

    Works for every exception type, including
    :class:`asyncio.CancelledError`, which cannot be slotted into the
    library's own exception hierarchy.
    """
    cast(_SendResultHolder, error).send_result = result


class SMTPSendState(enum.Enum):
    """
    How far a mail transaction progressed.

    The states fall into three groups with different retry implications:

    - Not submitted: :attr:`not_started`, :attr:`envelope_open` and
      :attr:`data_interrupted`. The end-of-data marker was never handed to
      the transport, so the server cannot have accepted the message.
    - Definitive reply received: :attr:`accepted` and :attr:`rejected`. The
      server's final answer is known.
    - Outcome unknown: :attr:`unknown`. The end-of-data marker was handed to
      the transport, but no final reply was observed; the server may already
      have accepted and delivered the message.
    """

    #: No command for this transaction was sent.
    not_started = enum.auto()
    #: MAIL FROM (and possibly some RCPT TO) was accepted, but DATA was not
    #: fully transmitted.
    envelope_open = enum.auto()
    #: The DATA command itself, or the message upload, failed before the
    #: end-of-data marker was handed to the transport.
    data_interrupted = enum.auto()
    #: The end-of-data marker was handed to the transport and a final 250
    #: reply was received.
    accepted = enum.auto()
    #: A definitive refusal was received (e.g. a non-250 final reply).
    rejected = enum.auto()
    #: The end-of-data marker was handed to the transport, but the
    #: connection dropped, timed out or was cancelled before the final
    #: reply was observed. Delivery may or may not have happened.
    unknown = enum.auto()

    @property
    def submitted(self) -> bool:
        """
        True once the end-of-data marker has been handed to the transport.

        Both :attr:`accepted` and :attr:`rejected` are past this boundary;
        the distinction is that a rejected transaction has a definitive
        answer and may be handled without manual reconciliation.
        """
        return self in (
            SMTPSendState.accepted,
            SMTPSendState.rejected,
            SMTPSendState.unknown,
        )

    @property
    def definitive(self) -> bool:
        """True if the server's final answer was observed."""
        return self in (SMTPSendState.accepted, SMTPSendState.rejected)


@dataclass
class SMTPSendResult:
    """
    Progress of a single :meth:`.SMTP.sendmail` transaction.

    Only envelope information (sender and recipient addresses) and server
    replies are stored; the message body is never retained here.

    Instances are exposed as :attr:`.SMTP.last_send_result` and attached to
    any exception raised by :meth:`.SMTP.sendmail` (including
    :class:`asyncio.CancelledError`) as its ``send_result`` attribute.
    """

    sender: str | None = None
    recipients: tuple[str, ...] = ()
    #: Recipients whose RCPT TO was accepted (250/251). Acceptance is an
    #: envelope acknowledgement only, not proof of delivery.
    accepted_recipients: tuple[str, ...] = ()
    #: Recipients whose RCPT TO was refused, with the server's reply.
    refused_recipients: dict[str, SMTPResponse] = field(default_factory=dict)
    state: SMTPSendState = SMTPSendState.not_started
    #: Final reply to DATA, when :attr:`state` is accepted or rejected.
    response: SMTPResponse | None = None
    #: What ended the transaction, when it did not complete successfully.
    error: BaseException | None = None

    def mark_envelope_open(self, sender: str, recipients: tuple[str, ...]) -> None:
        if self.state is SMTPSendState.not_started:
            self.sender = sender
            self.recipients = recipients
            self.state = SMTPSendState.envelope_open

    def mark_recipient_accepted(self, recipient: str) -> None:
        self.accepted_recipients = (*self.accepted_recipients, recipient)

    def mark_recipient_refused(self, recipient: str, response: SMTPResponse) -> None:
        self.refused_recipients[recipient] = response

    def mark_data_interrupted(self, error: BaseException) -> None:
        if not self.state.submitted:
            self.state = SMTPSendState.data_interrupted
            self.error = error

    def mark_submitted(self) -> None:
        # The end-of-data marker is on the wire; the final reply is unknown
        # until mark_accepted/mark_rejected is called.
        if self.state in (
            SMTPSendState.not_started,
            SMTPSendState.envelope_open,
            SMTPSendState.data_interrupted,
        ):
            self.state = SMTPSendState.unknown

    def mark_accepted(self, response: SMTPResponse) -> None:
        self.state = SMTPSendState.accepted
        self.response = response
        self.error = None

    def mark_rejected(self, response: SMTPResponse, error: BaseException) -> None:
        self.state = SMTPSendState.rejected
        self.response = response
        self.error = error

    def mark_failed(self, error: BaseException) -> None:
        """
        Record how an unfinished transaction ended without advancing past
        the confirmation boundary already reached.
        """
        if self.error is None:
            self.error = error
