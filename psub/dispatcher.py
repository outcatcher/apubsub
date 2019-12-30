import logging
import signal
from multiprocessing import Event, JoinableQueue, Manager, Pipe, Process, Queue, get_logger
from queue import Empty
from threading import Thread
from typing import Dict, NamedTuple, Set
from uuid import uuid4

LOGGER = get_logger()
LOGGER.setLevel(logging.DEBUG)
S_H = logging.StreamHandler()
S_H.setLevel(logging.INFO)
LOGGER.addHandler(S_H)

QUEUE_MAX_SIZE = 30

Topics = Dict[str, Set[Queue]]


class TopicSubscription(NamedTuple):
    topic: str
    queue: Queue


class Message(NamedTuple):
    topic: str
    data: str


class Service:
    """Publish / subscribe service"""

    # queues for interacting with service
    _publish_queue: JoinableQueue
    _subscribe_queue: JoinableQueue
    _unsubscribe_queue: JoinableQueue
    _client_registration_pipe: Pipe

    topics: Topics
    _clients: Dict[str, Queue]

    def __init__(self):
        self._publish_queue, self._subscribe_queue, self._unsubscribe_queue = \
            JoinableQueue(), JoinableQueue(), JoinableQueue()
        self._stop_event = Event()
        self._srv_process = Process(target=self._consume_input)
        self._client_end, self._service_end = Pipe()

    # noinspection PyBroadException
    def __consume_publish(self):
        LOGGER.info("Start consuming pub")
        while not self._stop_event.is_set():
            LOGGER.debug("Waiting for publish event")
            try:
                message = self._publish_queue.get(1)
            except Empty:
                continue
            if message.topic not in self.topics:
                LOGGER.warning("Topic %s does not exist: %s (no clients subscribed yet)", message.topic, self.topics)
                continue
            for queue in self.topics[message.topic]:
                queue.put(message.data)
            self._publish_queue.task_done()
        self._publish_queue.close()

    # noinspection PyBroadException
    def __consume_subscribe(self):
        LOGGER.info("Start consuming sub")
        while not self._stop_event.is_set():
            try:
                subscription: TopicSubscription = self._subscribe_queue.get(timeout=1)
            except Empty:
                continue
            try:
                self.topics[subscription.topic].add(subscription.queue)
            except KeyError:
                self.topics[subscription.topic] = {subscription.queue}
            except Exception:
                LOGGER.exception("Failed to process subscribe message: %s", subscription)
            self._subscribe_queue.task_done()
        self._subscribe_queue.close()

    # noinspection PyBroadException
    def __consume_unsubscribe(self):
        LOGGER.info("Start consuming unsubscribe")
        while not self._stop_event.is_set():
            try:
                subscription: TopicSubscription = self._unsubscribe_queue.get(timeout=1)
            except Empty:
                continue
            try:
                self.topics[subscription.topic].remove(subscription.queue)
            except KeyError:
                pass
            except Exception:
                LOGGER.exception("Failed to process unsubscribe message: %s", subscription)
            self._unsubscribe_queue.task_done()
        self._unsubscribe_queue.close()

    def __client_registration(self):
        LOGGER.info("Start consuming client registration")
        while not self._stop_event.is_set():
            try:
                self._service_end.poll(1)
            except TimeoutError:
                continue
            uuid = self._service_end.recv()
            if uuid not in self._clients:
                manager = Manager()
                self._clients[uuid] = manager.Queue()
            self._service_end.send(self._clients[uuid])

    def _consume_input(self):
        threads = [
            Thread(target=self.__consume_publish, name="Thread-PUB", daemon=True),
            Thread(target=self.__consume_subscribe, name="Thread-SUB", daemon=True),
            Thread(target=self.__consume_unsubscribe, name="Thread-UNSUB", daemon=True),
            Thread(target=self.__client_registration, name="Thread-REG", daemon=True),
        ]

        for thr in threads:
            thr.start()

        self._stop_event.wait()

        LOGGER.info("Joining queues...")
        for queue in [self._unsubscribe_queue, self._subscribe_queue, self._publish_queue]:
            queue.join()
        self._client_end.close()
        self._service_end.close()
        self._srv_process.close()

    def start(self):
        """Start pub/sub service"""
        signal.signal(signal.SIGTERM, self._stop_event.set)
        signal.signal(signal.SIGINT, self._stop_event.set)
        LOGGER.info("Starting service...")
        manager = Manager()
        self.topics: Topics = manager.dict({})
        self._clients: Dict[str, Queue] = manager.dict({})

        self._stop_event.clear()
        self._srv_process.start()

    def stop(self):
        """Stop pub/sub service"""
        LOGGER.info("Stopping service...")
        self._stop_event.set()
        self._srv_process.join()

    def subscribe(self, topic: str, client: str = None) -> Queue:
        """Create new queue or reuse existing for the client"""
        client = client or uuid4()
        self._client_end.send(client)
        client_queue = self._client_end.recv()
        self._subscribe_queue.put(TopicSubscription(topic, client_queue))
        return client_queue

    def unsubscribe(self, topic: str, client: str):
        """Remove queue from recipients of messages with given topic"""
        client_queue = self._clients[client]
        self._unsubscribe_queue.put(TopicSubscription(topic, client_queue))

    def publish(self, topic: str, data: str):
        """Publish message with topic"""
        LOGGER.info("Publish '%s' with topic '%s' to queue %s", data, topic, id(self._publish_queue))
        self._publish_queue.put(Message(topic, data))
