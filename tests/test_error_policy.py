from zerobus.sdk.shared import NonRetriableException, ZerobusException

from ais_producer.ais_source import AisSourceError
from ais_producer.error_policy import Action, decide


def test_non_retriable_zerobus_error_stops():
    assert decide(NonRetriableException("PERMISSION_DENIED"), stream_already_recreated=False) is Action.STOP


def test_retriable_zerobus_error_recreates_the_stream_once():
    assert decide(ZerobusException("stream failed"), stream_already_recreated=False) is Action.RECREATE_STREAM


def test_second_zerobus_failure_stops():
    assert decide(ZerobusException("stream failed"), stream_already_recreated=True) is Action.STOP


def test_invalid_ais_key_stops():
    assert decide(AisSourceError("Api Key Is Not Valid"), stream_already_recreated=False) is Action.STOP


def test_unknown_error_stops():
    assert decide(RuntimeError("bug"), stream_already_recreated=False) is Action.STOP
