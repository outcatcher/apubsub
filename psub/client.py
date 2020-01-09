import asyncio
import logging
import string
import time
from asyncio import Queue

from ._protocol import CMD_PUB, CMD_SUB, CMD_UNSUB, ENDIANNESS, OK, UTF8, command, ok, parse_cmd_response
from .connection_wrapper import receive, send

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

__all__ = ["ClientError", "Client"]


class ClientError(Exception):
    """Error message from service"""


class ServiceResponseError(Exception):
    """Fail during response parsing"""


def _port_to_bytes(port: int):
    return port.to_bytes(2, ENDIANNESS)


# noinspection PyBroadException
class Client:
    """Client for interacting with service"""

    _active = False

    def __init__(self, server_port: int, client_port: int):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        self._data_queue = Queue()
        self.server_port = server_port

        loop.run_until_complete(asyncio.start_server(self.consume_input, "localhost", client_port))
        self.port = client_port

    async def consume_input(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        message = await receive(reader)
        await self._data_queue.put(message)
        await send(writer, ok(b"", b""))
        writer.close()
        await writer.wait_closed()

    async def _send_command(self, cmd, topic, data=None):
        message = command(cmd, topic, data)
        reader, writer = await asyncio.open_connection("localhost", self.server_port)
        await send(writer, message)
        resolution, response = parse_cmd_response(await receive(reader))
        if resolution != OK:
            raise ClientError(f"CMD response is {resolution}, but {OK} expected")
        if cmd != response.command:
            raise ClientError(f"Expected response to {cmd} command, got {response.command}")
        writer.close()
        await writer.wait_closed()

    def send_command(self, cmd, topic, data):
        asyncio.get_event_loop().run_until_complete(self._send_command(cmd, topic, data))

    def publish(self, topic: str, data: str):
        self.send_command(CMD_PUB, topic, data)

    def subscribe(self, topic: str):
        if not (set(topic) < set(string.ascii_letters + string.digits)):
            raise TypeError("Topic can be only ASCII letters")
        self.send_command(CMD_SUB, topic, _port_to_bytes(self.port))

    def unsubscribe(self, topic: str):
        self.send_command(CMD_UNSUB, topic, _port_to_bytes(self.port))

    def get_single(self):
        """Get single data message"""
        if self._data_queue.empty():
            return None
        data: bytes = self._data_queue.get_nowait()
        return data.decode(UTF8)

    def start_receiving(self):
        """Get all received messages"""
        self._active = True
        while self._active:
            if not self._data_queue.empty():
                yield self._data_queue.get_nowait().decode(UTF8)
            time.sleep(0.01)

    def stop_receiving(self):
        self._active = False
