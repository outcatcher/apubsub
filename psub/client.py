import logging
from multiprocessing.connection import Connection
from typing import Generator

from ._protocol import pub, sub, unsub

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


# noinspection PyBroadException
class Client:
    """Client for interacting with service"""

    _active = False

    def __init__(self, uuid: str, _connection: Connection):
        self.uuid = uuid
        self._connection = _connection
        self._active = False

    def publish(self, topic, data):
        self._connection.send_bytes(pub(topic, data))

    def subscribe(self, topic):
        self._connection.send_bytes(sub(topic))

    def unsubscribe(self, topic):
        self._connection.send_bytes(unsub(topic))

    def start_receiving(self) -> Generator:
        """Receive messages from connection as generator"""
        while self._active:
            try:
                self._connection.poll(1)
            except TimeoutError:
                continue
            try:
                message = self._connection.recv_bytes()
                yield message.decode("utf8")
            except UnicodeDecodeError:
                LOGGER.exception("Can't decode message")
            except EOFError:
                break
            except Exception:
                LOGGER.exception("Can't receive message from connection")
                continue
        self._connection.close()
