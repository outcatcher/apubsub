import logging
from multiprocessing.connection import Connection
from typing import Generator

from ._protocol import ERR, parse_command, pub, sub, unsub

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class ClientError(Exception):
    """Error message from service"""


# noinspection PyBroadException
class Client:
    """Client for interacting with service"""

    _active = False

    def __init__(self, uuid: str, _connection: Connection):
        self.uuid = uuid
        self._connection = _connection
        self._active = False

    def _send_message(self, message: bytes):
        self._connection.send_bytes(message)
        response = self._connection.recv_bytes()
        cmd = parse_command(response)  # error message will be in `ParsedMessage.topic`
        if cmd.command == ERR:
            raise ClientError(cmd.topic)

    def publish(self, topic, data):
        self._send_message(pub(topic, data))

    def subscribe(self, topic):
        self._send_message(sub(topic))

    def unsubscribe(self, topic):
        self._send_message(unsub(topic))

    def start_receiving(self) -> Generator:
        """Receive messages from connection as generator"""
        self._active = True
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

    def stop(self):
        self._active = False
