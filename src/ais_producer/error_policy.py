"""Decides how the producer reacts to an error. Decides only; the sink and main carry the action out.

Short failures never reach this module: the Zerobus SDK and the websockets library retry
them by themselves. Only what they could not recover from is decided here.
"""

from enum import Enum

from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer.ais_source import AisSourceError


class Action(Enum):
    RECREATE_STREAM = "recreate_stream"
    STOP = "stop"


def decide(error: BaseException, stream_already_recreated: bool) -> Action:
    # Order matters: NonRetriableException is a subclass of ZerobusException.
    if isinstance(error, NonRetriableException):
        return Action.STOP  # wrong credentials or rights, missing table, schema mismatch
    if isinstance(error, ZerobusException):
        # The SDK's own recovery gave up: one fresh stream, then stop and let the supervisor or alert take over.
        return Action.STOP if stream_already_recreated else Action.RECREATE_STREAM
    if isinstance(error, AisSourceError):
        return Action.STOP  # aisstream refused the subscription, e.g. an invalid API key
    return Action.STOP  # unknown errors: stop loudly instead of guessing
