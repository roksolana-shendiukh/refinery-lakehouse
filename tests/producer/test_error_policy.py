from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer.ais_source import AisSourceError
from ais_producer.error_policy import Action, decide


def test_non_retriable_zerobus_error_stops():
    assert decide(NonRetriableException("PERMISSION_DENIED")) is Action.STOP


def test_retriable_zerobus_error_retries():
    assert decide(ZerobusException("stream failed")) is Action.RETRY


def test_invalid_ais_key_stops():
    assert decide(AisSourceError("Api Key Is Not Valid")) is Action.STOP


def test_unknown_error_stops():
    assert decide(RuntimeError("bug")) is Action.STOP
