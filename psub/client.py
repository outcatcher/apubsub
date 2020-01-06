import logging
from multiprocessing.connection import Connection
from typing import Generator, Optional

from ._protocol import UTF8, get_data, is_data, is_ok, parse_command, pub, sub, unsub

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
        self._cache = []

    def _send_message(self, message: bytes):
        self._connection.send_bytes(message)
        response = self._connection.recv_bytes()
        while is_data(response):
            self._cache.append(get_data(response))
            response = self._connection.recv_bytes()
        if not is_ok(response):
            cmd = parse_command(response)  # error message will be in `ParsedMessage.topic`
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
            if self._cache:
                yield self._cache.pop(0)
            try:
                ready = self._connection.poll(1)
                if not ready:
                    continue
            except BrokenPipeError:
                break
            try:
                message = get_data(self._connection.recv_bytes())
                yield message.decode(UTF8)
            except UnicodeDecodeError:
                LOGGER.exception("Can't decode message")
            except EOFError:
                break
            except Exception:
                LOGGER.exception("Can't receive message from connection")
                continue

    def stop_receiving(self):
        self._active = False

    def receive_single(self, timeout=0.0) -> Optional[str]:
        """Receive single message from pipe"""
        if not self._connection.poll(timeout):
            return None
        message = self._connection.recv_bytes()
        assert is_data(message)
        _, message = message.split(b"::")
        return message.decode(UTF8)
