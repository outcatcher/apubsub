import string

import pytest

from apubsub import Service
from tests.helpers import rand_str, started_client


@pytest.fixture(scope="session")
def service():
    srv = Service()
    srv.start()
    yield srv
    srv.stop()


@pytest.fixture
def data():
    return rand_str(200, string.printable)


@pytest.fixture
def topic():
    return rand_str(10, string.ascii_letters + string.digits)


@pytest.fixture
async def sub(service):
    return await started_client(service)


@pytest.fixture
async def pub(service):
    return await started_client(service)
