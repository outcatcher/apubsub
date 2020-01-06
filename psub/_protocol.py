"""Implementation of internal client-server protocol"""
from typing import AnyStr, Iterable, List, NamedTuple

UTF8 = "utf-8"
DIVIDER = b"::"


def _convert_to_bytes(*args: Iterable[AnyStr]) -> List[bytes]:
    b_args = []
    for arg in args:
        if isinstance(arg, str):
            b_args.append(arg.encode(UTF8))
        elif isinstance(arg, bytes):
            b_args.append(arg)
        else:
            raise TypeError(f"Unexpected argument {arg} of type {type(arg)}")
    return b_args


# Requests from client to server

CMD_PUB = b"PUB"
CMD_SUB = b"SUB"
CMD_UNSUB = b"USUB"


def build_message(command: AnyStr, data: AnyStr) -> bytes:
    command, data = _convert_to_bytes(command, data)
    return b"::".join([command, data])


class ParsedMessage(NamedTuple):
    command: bytes
    topic: bytes
    data: bytes = b""


class ParsingError(Exception):
    """Parsing failed"""


def parse_command(message: bytes) -> ParsedMessage:
    """Parse <command>::<topic>[,data] string as command"""
    command, data = message.split(DIVIDER, 1)
    topic, *data = data.split(b",", 1)
    return ParsedMessage(command, topic, *data)


def pub(topic: AnyStr, data: AnyStr):
    """Publish data to existing topic message"""

    return build_message(CMD_PUB, b",".join(_convert_to_bytes(topic, data)))


def sub(topic: AnyStr):
    """Subscribe to topic message"""
    if isinstance(topic, str):
        topic = topic.encode(UTF8)
    return build_message(CMD_SUB, topic)


def unsub(topic: AnyStr):
    """Unsubscribe from topic message"""
    if isinstance(topic, str):
        topic = topic.encode(UTF8)
    return build_message(CMD_UNSUB, topic)


# Response from server to clients

OK = b"OK"
ERR = b"ERR"
DATA = b"DATA"


def data_message(message: bytes) -> bytes:
    return DATA + DIVIDER + message


def get_data(message: bytes) -> bytes:
    if not is_data(message):
        raise ParsingError
    return message[len(DATA) + len(DIVIDER):]


def ok(*args: AnyStr) -> bytes:
    """Message processed"""
    message = b",".join(_convert_to_bytes(*args))
    return OK + DIVIDER + message


def err(*args: AnyStr) -> bytes:
    """Error during message processing"""
    message = b",".join(_convert_to_bytes(*args))
    return ERR + DIVIDER + message


def is_ok(message: bytes) -> bool:
    """Check if message is 'OK' message"""
    return message.startswith(OK + DIVIDER)


def is_data(message: bytes) -> bool:
    """Check if message is data message"""
    return message.startswith(DATA + DIVIDER)
