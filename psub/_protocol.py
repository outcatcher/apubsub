"""Implementation of internal client-server protocol"""
from typing import AnyStr, Iterable, List, NamedTuple, Optional, Tuple
from zlib import adler32

UTF8 = "utf-8"
MESSAGE_START = b"\01"
SEPARATOR = b"::"
SUB_SEPARATOR = b","
MAX_PACKET_SIZE = 0xffffff
ENDIANNESS = "big"


def _calc_min_bytes(value: int):
    size = 0
    while value > 0:
        value = value >> 8
        size += 1
    return size


PACKET_SIZE_SIZE = _calc_min_bytes(MAX_PACKET_SIZE)  # size of `size` message part


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


class MaxSizeOverflow(Exception):
    """Message is bigger than can be processed by protocol"""


def build_packet(body: bytes) -> bytes:
    if b"\00" in body:
        raise TypeError("Data should not contain NULL character")
    adler_size = 4
    body_hash = adler32(body).to_bytes(adler_size, ENDIANNESS)  # 4 bytes of checksum
    size = len(body) + adler_size
    if size > MAX_PACKET_SIZE:
        raise MaxSizeOverflow
    size = size.to_bytes(PACKET_SIZE_SIZE, ENDIANNESS, signed=False)
    return MESSAGE_START + size + body + body_hash


def _build_message(cmd: AnyStr, data: AnyStr) -> bytes:
    cmd, data = _convert_to_bytes(cmd, data)
    return cmd + SEPARATOR + data


class ParsedMessage(NamedTuple):
    command: bytes
    topic: bytes
    data: bytes = b""


class ParsingError(Exception):
    """Parsing failed"""


def parse_command(message: bytes) -> ParsedMessage:
    """Parse <command>::<topic>[,data] string as command"""
    cmd, data = message.split(SEPARATOR, 1)
    topic, *data = data.split(SUB_SEPARATOR, 1)
    return ParsedMessage(cmd, topic, *data)


def command(cmd: AnyStr, topic: AnyStr, data: Optional[AnyStr] = None):
    """Command message, e.g. b'SUB::topic,data'"""
    cmd, topic = _convert_to_bytes(cmd, topic)
    if data is None:
        return _build_message(cmd, topic)
    data = _convert_to_bytes(data)[0]
    return _build_message(cmd, topic + SUB_SEPARATOR + data)


# Response from server to clients

OK = b"OK"
ERR = b"ERR"
DATA = b"DATA"


def parse_cmd_response(message: bytes) -> Tuple[bytes, ParsedMessage]:
    verdict, data = message.split(SEPARATOR)
    cmd, topic, *other = data.split(SUB_SEPARATOR, 2)
    data = ParsedMessage(cmd, topic, other)
    return verdict, data


def ok(cmd, topic, *args: AnyStr) -> bytes:
    """Message processed"""
    message = SUB_SEPARATOR.join(_convert_to_bytes(cmd, topic, *args))
    return OK + SEPARATOR + message


def err(cmd, topic, *args: AnyStr) -> bytes:
    """Error during message processing"""
    message = SUB_SEPARATOR.join(_convert_to_bytes(cmd, topic, *args))
    return ERR + SEPARATOR + message


def is_ok(message: bytes) -> bool:
    """Check if message is 'OK' message"""
    return message.startswith(OK + SEPARATOR)


def is_data(message: bytes) -> bool:
    """Check if message is data message"""
    return message.startswith(DATA + SEPARATOR)
