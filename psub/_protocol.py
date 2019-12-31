"""Implementation of internal client-server protocol"""
from typing import AnyStr, Iterable, List, NamedTuple

UTF8 = "utf-8"


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


# Requests

CMD_PUB = b"PUB"
CMD_SUB = b"SUB"
CMD_UNSUB = b"USUB"


def build_message(command: AnyStr, data: AnyStr) -> bytes:
    command, data = _convert_to_bytes([command, data])
    return b"::".join([command, data])


class ParsedMessage(NamedTuple):
    command: bytes
    topic: bytes
    data: bytes = b""


def parse_command(message: bytes) -> ParsedMessage:
    """Parse <command>::<topic>[,data] string as command"""
    command, data = message.split(b"::", 1)
    topic, *data = data.split(b",", 1)
    return ParsedMessage(command, topic, data or b"")


def pub(topic: AnyStr, data: AnyStr):
    return build_message(CMD_PUB, b",".join(_convert_to_bytes([topic, data])))


def sub(topic: AnyStr):
    if isinstance(topic, str):
        topic = topic.encode(UTF8)
    return build_message(CMD_SUB, topic)


def unsub(topic: AnyStr):
    if isinstance(topic, str):
        topic = topic.encode(UTF8)
    return build_message(CMD_UNSUB, topic)


# Responses

OK = b"OK"
ERR = b"ERR"


def ok(*args: AnyStr) -> bytes:
    message = b",".join(_convert_to_bytes(args))
    message = b"::".join([OK, message])
    return message


def err(*args: AnyStr) -> bytes:
    message = b",".join(_convert_to_bytes(args))
    message = b"::".join([ERR, message])
    return message


def is_ok(message: bytes) -> bool:
    return message.startswith(b"%b::" % OK)
