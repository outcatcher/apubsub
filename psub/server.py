import logging
import time
from multiprocessing import Event, Lock, Pipe, Process, synchronize
from multiprocessing.connection import Connection
from threading import Thread
from typing import Dict, List, Set
from uuid import uuid4

from ._protocol import CMD_PUB, CMD_SUB, CMD_UNSUB, UTF8, err, ok, parse_command
from .client import Client

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


# noinspection PyBroadException
class Service:
    """Pub/sub service"""

    __run_lock: synchronize.Lock = Lock()
    __stop: Event = Event()
    __topics: Dict[str, Set[str]]
    __clients: Dict[str, Connection]
    __cl_threads: List[Thread]
    _service_p: Process

    def __init__(self):
        self.__clients = {}
        self.__topics = {}
        self.__cl_threads = []
        self._service_p = Process(target=self._start)

    def get_client(self) -> Client:
        """Get new client instance for running server"""
        client_end, service_end = Pipe()
        uuid = str(uuid4())
        self.__clients[uuid] = service_end
        self._start_handle_client(uuid)
        return Client(uuid, client_end)

    def client_registered(self, client: Client) -> bool:
        """Check if client with given uuid exists"""
        return client.uuid in self.__clients

    def _start(self):
        if not self.__run_lock.acquire(block=False):
            raise RuntimeError("Lock is currently acquired by other process")
        _topics = {}
        while not self.__stop:
            time.sleep(0.1)
        self.__run_lock.release()

    def __handle_sub(self, topic: str, client_uuid: str) -> bytes:
        response = ok(CMD_SUB, topic, client_uuid)
        try:
            self.__topics[topic].add(client_uuid)
        except KeyError:
            self.__topics[topic] = {client_uuid}
        except Exception:
            error_message = f"Unhandled exception while subscribing `{client_uuid}` to `{topic}`"
            LOGGER.exception(error_message)
            response = err(CMD_SUB, topic, client_uuid)
        return response

    def __handle_unsub(self, topic: str, client_uuid: str) -> bytes:
        response = ok(CMD_UNSUB, topic, client_uuid)
        try:
            self.__topics[topic].remove(client_uuid)
        except KeyError:
            pass
        except Exception:
            error_message = f"Unhandled exception while un-subscribing `{client_uuid}` from `{topic}`"
            LOGGER.exception(error_message)
            response = err(CMD_UNSUB, topic, client_uuid)
        return response

    def __handle_pub(self, topic: str, data: bytes) -> bytes:
        """Send data for all not closed connections in self._topics"""
        try:
            subscribed_clients = self.__topics[topic]
        except KeyError:
            message = f"Not topic {topic} exists"
            LOGGER.exception(message)
            return err(CMD_PUB, topic)
        for client in subscribed_clients:
            connection = self.__clients[client]
            if not connection.closed:
                connection.send_bytes(data)
        return ok(CMD_PUB, topic)

    def __handle_client(self, _uuid):
        connection = self.__clients[_uuid]
        while not self.__stop.is_set():
            try:
                ready = connection.poll(0.1)
            except BrokenPipeError:
                break
            if not ready:
                continue
            try:
                data = connection.recv_bytes()
            except EOFError:
                LOGGER.warning("Server connection is closed, stopping client")
                break
            command = parse_command(data)
            response = err(b"Unknown command", command.command)
            topic = command.topic.decode(UTF8)
            if command.command == CMD_SUB:
                response = self.__handle_sub(topic, _uuid)
            elif command.command == CMD_UNSUB:
                response = self.__handle_unsub(topic, _uuid)
            elif command.command == CMD_PUB:
                response = self.__handle_pub(topic, command.data)
            connection.send_bytes(response)
        connection.close()

    def _start_handle_client(self, client: str):
        """Handle client communication in new thread"""
        thr = Thread(target=self.__handle_client, name=f"ClientThread-{client}", args=(client,))
        thr.start()
        self.__cl_threads.append(thr)

    def start(self):
        self._service_p.start()
        LOGGER.info("Service process started")

    def stop(self):
        self.__stop.set()
        self._service_p.join()
        for thr in self.__cl_threads:
            thr.join()
        LOGGER.info("Service process stopped")
