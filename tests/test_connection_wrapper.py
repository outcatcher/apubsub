import random
from zlib import adler32

import pytest

from apubsub.connection_wrapper import NotMessage, validate_checksum
from apubsub.protocol import ADLER_SIZE, ENDIANNESS, UTF8
from tests.helpers import rand_str


@pytest.fixture
def data():
    return rand_str().encode(UTF8)


@pytest.fixture
def checksum(data):
    return adler32(data).to_bytes(ADLER_SIZE, ENDIANNESS)


def test_message_cache_validation(data, checksum):
    validate_checksum(data, checksum)


def test_message_invalid_checksum(data):
    crc = random.randrange(0xffff).to_bytes(2, ENDIANNESS)
    with pytest.raises(NotMessage):
        validate_checksum(data, crc)
