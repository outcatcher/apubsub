"""Implementation of internal client-server protocol"""
from typing import AnyStr, Iterable, List, NamedTuple, Optional, Tuple

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


def build_message(cmd: AnyStr, data: AnyStr) -> bytes:
    cmd, data = _convert_to_bytes(cmd, data)
    return b"::".join([cmd, data])


class ParsedMessage(NamedTuple):
    command: bytes
    topic: bytes
    data: bytes = b""


class ParsingError(Exception):
    """Parsing failed"""


def parse_command(message: bytes) -> ParsedMessage:
    """Parse <command>::<topic>[,data] string as command"""
    cmd, data = message.split(DIVIDER, 1)
    topic, *data = data.split(b",", 1)
    return ParsedMessage(cmd, topic, *data)


def command(cmd: AnyStr, topic: AnyStr, data: Optional[AnyStr] = None):
    """Command message, e.g. b'SUB::topic,data'"""
    cmd, topic = _convert_to_bytes(cmd, topic)
    if data is None:
        return build_message(cmd, topic)
    data = _convert_to_bytes(data)[0]
    return build_message(cmd, topic + SUB_SEPARATOR + data)


# Response from server to clients

OK = b"OK"
ERR = b"ERR"
DATA = b"DATA"
SUB_SEPARATOR = b","


def data_message(message: bytes) -> bytes:
    return DATA + DIVIDER + message


def get_data(message: bytes) -> bytes:
    if not is_data(message):
        raise ParsingError
    return message[len(DATA) + len(DIVIDER):]


def parse_cmd_response(message: bytes) -> Tuple[bytes, ParsedMessage]:
    verdict, data = message.split(DIVIDER)
    cmd, topic, *other = data.split(SUB_SEPARATOR, 2)
    data = ParsedMessage(cmd, topic, other)
    return verdict, data


def ok(cmd, topic, *args: AnyStr) -> bytes:
    """Message processed"""
    message = SUB_SEPARATOR.join(_convert_to_bytes(cmd, topic, *args))
    return OK + DIVIDER + message


def err(cmd, topic, *args: AnyStr) -> bytes:
    """Error during message processing"""
    message = SUB_SEPARATOR.join(_convert_to_bytes(cmd, topic, *args))
    return ERR + DIVIDER + message


def is_ok(message: bytes) -> bool:
    """Check if message is 'OK' message"""
    return message.startswith(OK + DIVIDER)


def is_data(message: bytes) -> bool:
    """Check if message is data message"""
    return message.startswith(DATA + DIVIDER)
