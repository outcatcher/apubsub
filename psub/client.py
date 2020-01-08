import logging
from multiprocessing.connection import Connection
from typing import Generator, List, Optional

from ._protocol import (
    CMD_PUB, CMD_SUB, CMD_UNSUB, OK, ParsedMessage, UTF8, command, get_data, is_data, parse_cmd_response
)

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

__all__ = ["ClientError", "Client"]


class ClientError(Exception):
    """Error message from service"""


class ServiceResponseError(Exception):
    """Fail during response parsing"""


# noinspection PyBroadException
class Client:
    """Client for interacting with service"""

    _active = False

    def __init__(self, _uuid: str, _connection: Connection):
        self.uuid = _uuid
        self._connection = _connection
        self._active = False
        self._cache: List[bytes] = []  # cache for data messages

    def __receive_cmd_response(self, poll_timeout=1.0) -> ParsedMessage:
        def _poll():
            if not self._connection.poll(poll_timeout):
                raise TimeoutError(f"Service is not responding in {poll_timeout} seconds")

        _poll()
        message = self._connection.recv_bytes()

        while is_data(message):  # consume received data messages
            self._cache.append(message[:-1])
            _poll()
            message = self._connection.recv_bytes()

        verdict, cmd = parse_cmd_response(message)
        if verdict != OK:
            raise ClientError(f"Fail during performing '{cmd.command}' on '{cmd.topic}'")
        return cmd

    def _send_command(self, cmd, topic, data=None):
        message = command(cmd, topic, data)
        self._connection.send_bytes(message)
        response = self.__receive_cmd_response()
        if cmd != response.command:
            raise ClientError(f"Expected response to {cmd} command, got {response.command}")

    def publish(self, topic: str, data: str):
        self._send_command(CMD_PUB, topic, data)

    def subscribe(self, topic: str):
        self._send_command(CMD_SUB, topic)

    def unsubscribe(self, topic: str):
        self._send_command(CMD_UNSUB, topic)

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
                message = get_data(self._connection.recv_bytes())[:-1]
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

    def receive_single(self) -> Optional[str]:
        """Receive single message from pipe"""
        if not self._cache:
            if not self._connection.poll(1):
                return None
            message = self._connection.recv_bytes()[:-1]
        else:
            message = self._cache.pop(0)

        assert is_data(message), f"Expected data message, got:\n`{message}`"
        return get_data(message).decode(UTF8)
