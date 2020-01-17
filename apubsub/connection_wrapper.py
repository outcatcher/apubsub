"""High-level methods for message processing"""

import asyncio
import logging
from zlib import adler32

from apubsub.protocol import ENDIANNESS, MESSAGE_START, build_packet


class NotMessage(Exception):
    """Received data is not a message"""


class NoData(Exception):
    """No data received, but connection is closed"""


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


def validate_checksum(data: bytes, check_bytes: bytes):
    b_hash = int.from_bytes(check_bytes, ENDIANNESS)
    if adler32(data) != b_hash:
        raise NotMessage(f"Invalid message checksum")


async def receive(reader: asyncio.StreamReader) -> bytes:
    """Receive bytes message"""
    first = b""
    while not first:
        first = await reader.read(1)
        if reader.at_eof():
            raise NoData
    if first[:1] != MESSAGE_START:
        raise NotMessage(f"No start bytes found. All data in reader: {await reader.read(1024)}")

    size = int.from_bytes(await reader.readexactly(3), ENDIANNESS)
    body = await reader.readexactly(size)
    data = body[:-4]
    validate_checksum(data, body[-4:])
    return bytes(data)


async def send(writer: asyncio.StreamWriter, data: bytes):
    """Send message to socket"""
    message = build_packet(data)
    writer.write(message)
    await writer.drain()
