import random
import socket
import string
import time

import pytest

from psub.server import Service


def _rand_str(size=10, charset=string.ascii_letters):
    r_string = "".join(random.choice(charset) for _ in range(size))
    return r_string


@pytest.fixture(scope="session")
def service():
    srv = Service()
    srv.start()
    sock = socket.socket(socket.AF_INET)
    sock.settimeout(3)
    time.sleep(0.5)
    try:
        sock.connect(("localhost", srv.port))
    finally:
        sock.close()
    yield srv
    srv.stop()


@pytest.fixture
def data():
    return _rand_str(200, string.printable)


@pytest.fixture
def topic():
    return _rand_str(10, string.ascii_letters + string.digits)


@pytest.fixture
def subs(service):
    count = random.randrange(2, 10)
    _subs = [service.get_client() for _ in range(count)]
    return _subs


@pytest.fixture
def pub(service):
    return service.get_client()
