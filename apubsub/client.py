import asyncio
import logging
import re
import time
from asyncio import Queue, QueueEmpty
from typing import List, Optional

from .connection_wrapper import receive, send
from .protocol import CMD_PUB, CMD_SUB, CMD_UNSUB, ENDIANNESS, OK, UTF8, command, ok, parse_cmd_response

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)

__all__ = ["ClientError", "Client"]


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

    _active: asyncio.Event

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
        self._active = asyncio.Event()

    async def consume_input(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Process input connections"""
        message = await receive(reader)
        await self._data_queue.put(message)
        await send(writer, ok(b"", b""))
        writer.close()
        await writer.wait_closed()

    async def _send_command(self, cmd, topic, data=None):
        message = command(cmd, topic, data)
        reader, writer = await asyncio.open_connection("localhost", self.server_port)
        try:
            await send(writer, message)
            resolution, response = parse_cmd_response(await receive(reader))
            if resolution != OK:
                raise ClientError(f"CMD failed with `{resolution.decode(UTF8)}`: `{response.data.decode(UTF8)}`")
            if cmd != response.command:
                raise ClientError(f"Expected response to {cmd} command, got {response.command}")
        finally:
            writer.close()
            await writer.wait_closed()

    def send_command(self, cmd, topic, data):
        """Send command to service"""
        asyncio.get_event_loop().run_until_complete(self._send_command(cmd, topic, data))

    def publish(self, topic: str, data: str):
        """Publish data to service"""
        self.send_command(CMD_PUB, topic, data)

    def subscribe(self, topic: str):
        """Subscribe client to a topic"""

        if ALLOWED_TOPIC_RE.fullmatch(topic) is None:
            raise TypeError("Topic can be only ASCII letters")
        self.send_command(CMD_SUB, topic, _port_to_bytes(self.port))

    def unsubscribe(self, topic: str):
        """Unsubscribe client from topic

        Previously published messages will still be available
        """
        self.send_command(CMD_UNSUB, topic, _port_to_bytes(self.port))

    def get_single(self, timeout=0.0) -> Optional[str]:
        """Get single data message from input queue

        Returning received message or None, if queue is empty.
        If ``timeout`` > 0, will wait for given seconds if input queue is empty
        """

        end_time = time.monotonic() + timeout
        while time.monotonic() <= end_time:
            if not self._data_queue.empty():
                data: bytes = self._data_queue.get_nowait()
                return data.decode(UTF8)
            time.sleep(.001)
        return None

    def get_all(self) -> List[str]:
        """Get all already received messages"""
        result = []
        while not self._data_queue.empty():
            msg = self._data_queue.get_nowait()
            result.append(msg.decode(UTF8))
        return result

    async def start_receiving(self):
        """Start async generator receiving published messages"""

        self._active.set()
        while self._active.is_set():
            try:
                data: bytes = self._data_queue.get_nowait()
            except QueueEmpty:
                await asyncio.sleep(.01)
                continue
            self._data_queue.task_done()
            yield data.decode(UTF8)
        remaining = self._data_queue.qsize()
        if remaining > 0:
            LOGGER.info("Remaining tasks: %s", remaining)

    def stop_receiving(self):
        """Stop async  receiving"""
        self._active.clear()
