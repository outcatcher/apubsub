import time
from multiprocessing import Event, JoinableQueue, Process, Queue
from threading import Thread
from typing import Dict, Set

QUEUE_MAX_SIZE = 30

_SUBSCRIBE_QUEUE = JoinableQueue(QUEUE_MAX_SIZE)
_UNSUBSCRIBE_QUEUE = JoinableQueue(QUEUE_MAX_SIZE)
_PUBLISH_QUEUE = JoinableQueue(QUEUE_MAX_SIZE)

ACTIVE = Event()


def subscribe(client_queue: Queue, topic: str):
    """Set queue to receive messages with given topic"""
    _SUBSCRIBE_QUEUE.put((client_queue, topic))


def unsubscribe(client_queue: Queue, topic: str):
    """Remove queue from recipients of messages with given topic"""
    _UNSUBSCRIBE_QUEUE.put((client_queue, topic))


def publish(topic: str, data: str):
    """Publish message with topic"""
    _PUBLISH_QUEUE.put((topic, data))


Topics = Dict[str, Set[Queue]]


def __consume_publish(topics: Topics):
    while ACTIVE.is_set():
        topic, data = _PUBLISH_QUEUE.get()


def __consume_subscribe(subscriptions):
    pass


def __consume_unsubscribe(subscriptions):
    pass


def _consume_input():
    ACTIVE.set()
    topics: Topics = {}
    subscriptions = {}
    threads = [Thread(target=__consume_publish, args=(topics,)),
               Thread(target=__consume_subscribe, args=(subscriptions,)),
               Thread(target=__consume_unsubscribe, args=(subscriptions,))]
    for thr in threads:
        thr.start()
    while ACTIVE.is_set():
        time.sleep(0.1)
    for thr in threads:
        thr.join()


def main():
    dispatcher_process = Process(target=_consume_input)
    try:
        dispatcher_process.start()
    except KeyboardInterrupt:
        ACTIVE.clear()
        dispatcher_process.join()


if __name__ == "__main__":
    main()
