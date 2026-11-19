"""Decides how the producer reacts to an error. Decides only; the sink and main carry the action out.

Short failures never reach this module: the Zerobus SDK and the websockets library retry
them by themselves. Only what they could not recover from is decided here.
"""

from enum import Enum

from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer.ais_source import AisSourceError


class Action(Enum):
    RETRY = "retry"  # wait (backoff), recreate the stream, send again; main stops when the budget is spent
    STOP = "stop"  # something is broken


def decide(error: BaseException) -> Action:
    if isinstance(error, NonRetriableException):
        return Action.STOP  # wrong credentials or rights, missing table, schema mismatch
    if isinstance(error, ZerobusException):
        return Action.RETRY  # the SDK's own recovery gave up: a fresh stream after a pause may work
    if isinstance(error, AisSourceError):
        return Action.STOP  # aisstream refused the subscription, e.g. an invalid API key
    return Action.STOP  # unknown errors: stop loudly instead of guessing
