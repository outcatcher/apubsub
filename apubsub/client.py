import asyncio
import logging
import re
from asyncio import Queue
from typing import List, Union

from .connection_wrapper import receive, send
from .protocol import CMD_PORT, CMD_PUB, CMD_SUB, CMD_UNSUB, ENDIANNESS, OK, UTF8, command, ok, parse_cmd_response

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

__all__ = ["ClientError", "Client", "LOCALHOST"]


class ClientError(Exception):
    """Error message from service"""


class ServiceResponseError(Exception):
    """Fail during response parsing"""


def _port_to_bytes(port: int):
    return port.to_bytes(2, ENDIANNESS)


ALLOWED_TOPIC_RE = re.compile(r"[\w_\-\d]+")


# noinspection PyBroadException
class Client:
    """Client for interacting with service"""

    _receiving: asyncio.Event
    port: int = None

    def __init__(self, server_port: int):
        self.__data_queue = None
        self.server_port = server_port
        self._receiving = asyncio.Event()

    @property
    def _data_queue(self) -> Queue:
        if self.__data_queue is None:
            raise ValueError(f"Consumer queue for client on port {self.port} is missing.\n"
                             "Call client.start_consuming() first")
        return self.__data_queue

    async def start_consuming(self):
        """Start TCP server receiving data from service"""
        self.__data_queue = Queue()
        self.port = await self.get_port()
        await asyncio.start_server(self._consume_input, LOCALHOST, self.port)
        await asyncio.sleep(.05)
        await asyncio.wait_for(asyncio.open_connection(LOCALHOST, self.port), 3)

    async def _consume_input(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Process input connections"""
        message = await receive(reader)
        await self._data_queue.put(message)
        await send(writer, ok(b"", b""))
        writer.close()
        await writer.wait_closed()

    async def send_command(self, cmd, topic, data: Union[bytes, str] = ""):
        """Send command to service"""
        message = command(cmd, topic, data)
        reader, writer = await asyncio.open_connection(LOCALHOST, self.server_port)
        try:
            await send(writer, message)
            resolution, response = parse_cmd_response(await receive(reader))
            if resolution != OK:
                raise ClientError(f"CMD failed with `{resolution.decode(UTF8)}`: `{response.data.decode(UTF8)}`")
            if cmd != response.command:
                raise ClientError(f"Expected response to {cmd} command, got {response.command}")  # pragma: no cover
            return response
        finally:
            writer.close()
            await writer.wait_closed()

    async def get_port(self):
        """Get port for the client"""
        if self.port is None:
            response = await self.send_command(CMD_PORT, "-")
            self.port = int(response.topic.decode(UTF8))
        return self.port

    async def publish(self, topic: str, data: str):
        """Publish data to service"""
        await self.get_port()
        await self.send_command(CMD_PUB, topic, data)

    async def subscribe(self, topic: str):
        """Subscribe client to a topic"""
        await self.get_port()
        if ALLOWED_TOPIC_RE.fullmatch(topic) is None:
            raise TypeError("Topic can be only ASCII letters")
        await self.send_command(CMD_SUB, topic, _port_to_bytes(self.port))

    async def unsubscribe(self, topic: str):
        """Unsubscribe client from topic

        Previously published messages will still be available
        """
        await self.send_command(CMD_UNSUB, topic, _port_to_bytes(self.port))

    async def get(self, timeout=0.0):
        """Get single data message from input queue

        Returning received message or None, if queue is empty.
        If ``timeout > 0``, will wait for given seconds if input queue is empty.
        If ``timeout is None``, will wait forever
        """
        try:
            data: bytes = await asyncio.wait_for(self._data_queue.get(), timeout)
        except asyncio.TimeoutError:
            return None
        return data.decode(UTF8)

    def get_all(self) -> List[str]:
        """Get all already received messages"""
        result = []
        while not self._data_queue.empty():
            msg = self._data_queue.get_nowait()
            result.append(msg.decode(UTF8))
        return result

    async def get_iter(self):
        """Start async generator receiving published messages"""

        self._receiving.set()
        while self._receiving.is_set():
            try:
                data: bytes = await asyncio.wait_for(self._data_queue.get(), .1)
            except asyncio.TimeoutError:
                continue
            self._data_queue.task_done()
            yield data.decode(UTF8)
        remaining = self._data_queue.qsize()
        if remaining > 0:
            LOGGER.info("Remaining tasks in queue: %s", remaining)  # pragma: no cover

    def stop_getting(self):
        """Stop async generator"""
        self._receiving.clear()


LOCALHOST = "127.0.0.1"
