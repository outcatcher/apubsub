"""Message service"""

import asyncio
import logging
import socket
from multiprocessing import Event, Lock, Process, synchronize
from typing import Dict, Set
from uuid import uuid4

from .client import Client
from .connection_wrapper import NoData, NotMessage, receive, send
from .protocol import CMD_PUB, CMD_SUB, CMD_UNSUB, ENDIANNESS, UTF8, err, ok, parse_command

try:  # pragma: no cover
    # noinspection PyUnresolvedReferences
    import uvloop

    uvloop.install()
except ImportError:   # pragma: no cover
    pass

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def _random_port():
    sock = socket.socket()
    sock.bind(("localhost", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def _send_singe(port, data):
    try:
        _, writer = await asyncio.open_connection("localhost", port)
    except ConnectionError:
        LOGGER.exception(f"Failed to send data to client {port}")
        return
    await send(writer, data)
    writer.close()
    await writer.wait_closed()


# noinspection PyBroadException
class Service:
    """Message service running in stand-alone process"""

    _stop: Event = Event()
    __run_lock: synchronize.SemLock = Lock()
    __topics: Dict[str, Set[int]]
    _service_p: Process

    async def _handle_pub(self, topic: str, data: bytes):
        _ok = ok(CMD_PUB, topic)
        try:
            clients = self.__topics[topic]
        except KeyError:
            return _ok
        sends = [_send_singe(port, data) for port in clients]
        await asyncio.wait(sends)
        return _ok

    async def _handle_sub(self, topic: str, port: int):
        try:
            self.__topics[topic].add(port)
        except KeyError:
            self.__topics[topic] = {port}
        return ok(CMD_SUB, topic)

    async def _handle_unsub(self, topic: str, port: int):
        try:
            self.__topics[topic].remove(port)
        except KeyError:
            pass
        return ok(CMD_UNSUB, topic)

    async def _handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            message = await receive(reader)
        except NotMessage:
            LOGGER.exception("Can't process received message")
            await send(writer, b"Invalid message")
            writer.close()
            await writer.wait_closed()
            return
        except NoData:
            writer.close()
            await writer.wait_closed()
            return  # client hasn't done anything

        command = parse_command(message)
        LOGGER.debug("Received command: %s", command)
        topic = command.topic.decode(UTF8)
        if command.command == CMD_PUB:
            response = await self._handle_pub(topic, command.data)
        elif command.command == CMD_SUB:
            response = await self._handle_sub(topic, int.from_bytes(command.data, ENDIANNESS))
        elif command.command == CMD_UNSUB:
            response = await self._handle_unsub(topic, int.from_bytes(command.data, ENDIANNESS))
        else:
            response = err(b"Unknown command", command.command)
        await send(writer, response)
        writer.close()
        await writer.wait_closed()

    def __init__(self):
        """Create new service instance"""
        self.__clients = {}
        self.__topics = {}
        self.port = _random_port()
        self._service_p = Process(target=self._serve, args=(Service._stop,))

    def get_client(self) -> Client:
        """Get new client instance for running server"""
        uuid = str(uuid4())
        client_port = _random_port()
        client = Client(self.port, client_port)
        self.__clients[uuid] = client_port
        return client

    def _serve(self, stop_event):
        loop = asyncio.get_event_loop()
        server = loop.run_until_complete(asyncio.start_server(self._handle_request, "localhost", self.port))
        LOGGER.debug("Server started")
        loop.run_until_complete(_wait_for_stop(server, stop_event))

    def start(self):
        """Start new service process"""

        self._stop.clear()
        self._service_p.start()
        LOGGER.info("Service started on port %s", self.port)

    def stop(self):
        """Stop running service process"""

        self._stop.set()
        self._service_p.join()
        LOGGER.info("Service process stopped")


async def _wait_for_stop(srv, event):
    while not event.is_set():
        await asyncio.sleep(.1)
    if srv is not None:
        srv.close()
        await srv.wait_closed()
