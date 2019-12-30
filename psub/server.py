import logging
from functools import partial
from multiprocessing import Event, Lock, Pipe, Process, synchronize
from multiprocessing.connection import Connection
from typing import Dict, Set
from uuid import uuid4

from .client import Client

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class Service:
    """Pub/sub service"""

    _run_lock: synchronize.Lock = Lock()
    _stop: Event = Event()
    _topics: Dict[str, Set[str]]
    _clients: Dict[str, Connection]

    def get_client(self) -> Client:
        """Get new client instance for running server"""

    def client_exist(self, client: Client) -> bool:
        """Check if client with given uuid exists"""

    def _start(self, lock, _clients):
        if not lock.acquire(block=False):
            raise RuntimeError("Lock is currently acquired by other process")
        _topics = {}

    def __init__(self):
        _clients = {}
        self.get_client = partial(_get_client, _clients=_clients)
        self.client_registered = partial(_client_registered, _clients=_clients)
        self.service_p = Process(target=self._start, args=(self._run_lock, _clients))

    def start(self):
        self.service_p.start()
        LOGGER.info("Service process started")

    def stop(self):
        self._stop.set()
        self.service_p.join()
        self._run_lock.release()
        LOGGER.info("Service process finished")


def _client_registered(client, _clients):
    return client.uuid in _clients


def _get_client(_clients):
    client_c, service_c = Pipe()
    uuid = str(uuid4())
    _clients[uuid] = client_c
    return Client(uuid, client_c)
