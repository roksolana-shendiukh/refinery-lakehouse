from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class AisMessage(_message.Message):
    __slots__ = ("received_at", "message_type", "raw_message")
    RECEIVED_AT_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_TYPE_FIELD_NUMBER: _ClassVar[int]
    RAW_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    received_at: int
    message_type: str
    raw_message: str
    def __init__(self, received_at: _Optional[int] = ..., message_type: _Optional[str] = ..., raw_message: _Optional[str] = ...) -> None: ...
