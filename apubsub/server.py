"""Message service"""

import asyncio
import logging
import socket
import time
from collections import deque
from multiprocessing import Event, Lock, Process, synchronize
from typing import Deque, Dict, Set
from uuid import uuid4

from .client import Client, LOCALHOST
from .connection_wrapper import NoData, NotMessage, receive, send
from .protocol import CMD_PORT, CMD_PUB, CMD_SUB, CMD_UNSUB, ENDIANNESS, UTF8, err, ok, parse_command

try:  # pragma: no cover
    # noinspection PyUnresolvedReferences
    import uvloop

    uvloop.install()
except ImportError:  # pragma: no cover
    pass

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


async def _send_singe(port, data):
    try:
        _, writer = await asyncio.open_connection(LOCALHOST, port)
    except ConnectionError:
        LOGGER.exception(f"Failed to send data to client {port}")
        return
    await send(writer, data)
    writer.close()
    await writer.wait_closed()


def port_busy(port: int) -> bool:
    """Check if someone is listening to the port"""
    sock = socket.socket()
    sock.settimeout(0.1)
    try:
        sock.connect((LOCALHOST, port))
    except (ConnectionError, TimeoutError, socket.timeout):
        return False
    return True


# noinspection PyBroadException
class Service:
    """Message service running in stand-alone process"""

    _stop: Event = Event()
    __run_lock: synchronize.SemLock = Lock()
    __topics: Dict[str, Set[int]]
    _service_p: Process
    _allowed_ports: Deque[int]
    port: int

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

    def _handle_port(self):
        port = self._allowed_ports.popleft()
        self.__clients[str(uuid4())] = port
        return ok(CMD_PORT, f"{port}")

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
        elif command.command == CMD_PORT:
            response = self._handle_port()
        else:
            response = err(b"Unknown command", command.command)
        await send(writer, response)
        writer.close()
        await writer.wait_closed()

    def __init__(self, service_port=58608):
        """Create new service instance"""
        self.__clients = {}
        self.__topics = {}
        while port_busy(service_port):
            service_port -= 110
        client_start_port = service_port + 1
        self.port = service_port
        self._allowed_ports = deque(range(client_start_port, client_start_port + 100))
        self._service_p = Process(target=self._serve, args=(Service._stop,))

    @property
    def address(self):
        return LOCALHOST, self.port

    def get_client(self) -> Client:
        """Get new client instance for running server"""
        client = Client(self.port)
        return client

    def _serve(self, stop_event):
        loop = asyncio.get_event_loop()
        server = loop.run_until_complete(asyncio.start_server(self._handle_request, *self.address))
        LOGGER.debug("Server started")
        loop.run_until_complete(_wait_for_stop(server, stop_event))

    def start(self):
        """Start new service process"""
        self._stop.clear()
        self._service_p.start()
        time.sleep(.2)
        with socket.socket(socket.AF_INET) as sock:
            sock.settimeout(5)
            sock.connect(self.address)
        LOGGER.info("Service started on %s", self.address)

    def stop(self):
        """Stop running service process"""
        self._stop.set()
        self._service_p.join()
        LOGGER.info("Service process stopped")


async def _wait_for_stop(srv, event):
    while not event.is_set():
        await asyncio.sleep(.1)
    srv.close()
    await srv.wait_closed()
