import asyncio
import string
import time
from threading import Thread

import pytest

from apubsub.client import Client, ClientError
from apubsub.protocol import CMD_PUB, MAX_PACKET_SIZE, MaxSizeOverflow
from tests.conftest import _rand_str


def test_multiple_subs(subs, pub, topic, data):
    for sub in subs:
        sub.subscribe(topic)
    pub.publish(topic, data)
    for sub in subs:
        assert sub.get_single(.1) == data


async def _receive_all(sub: Client):
    res = []
    async for v in sub.start_receiving():
        res.append(v)
        await asyncio.sleep(.01)
    return res


def test_receive_generator(service, pub, data, topic):
    count = 5
    sub = service.get_client()
    sub.subscribe(topic)
    generated = [_rand_str(200, string.printable) for _ in range(count)]
    for msg in generated:
        pub.publish(topic, msg)
    loop = asyncio.get_event_loop()
    loop.call_later(.1, sub.stop_receiving)
    received = loop.run_until_complete(_receive_all(sub))
    assert sorted(received) == sorted(generated)


def test_unsubscribe(service, pub, topic, subs, data):
    topic2 = "topic2"
    for sub in subs:
        sub.subscribe(topic)
        sub.subscribe(topic2)
    special_sub = subs.pop(0)
    special_sub.unsubscribe(topic2)

    data2 = "Second data"
    pub.publish(topic, data)
    pub.publish(topic2, data2)

    assert (special_sub.get_single(.1), special_sub.get_single(.1)) == (data, None)
    for sub in subs:
        assert (sub.get_single(.1), sub.get_single(.1)) == (data, data2)


def test_receive_big_data(service, pub, topic):
    sub = service.get_client()
    sub.subscribe(topic)
    sent = _rand_str(20_000, string.printable)
    pub.publish(topic, sent)
    received = sub.get_single(1)
    assert received == sent


def test_unknown_command(service):
    client = service.get_client()
    with pytest.raises(ClientError):
        client.send_command("UNKNOWN", "", "")


def test_too_big(pub):
    with pytest.raises(MaxSizeOverflow):
        pub.publish("topic", "A" * MAX_PACKET_SIZE + "A")


def test_not_allowed_topic(pub):
    with pytest.raises(TypeError):
        pub.subscribe("topic-1")


def test_bytes_command(pub):
    pub.subscribe("topic")
    with pytest.raises(TypeError):
        pub.send_command(CMD_PUB, b"topic", None)


def test_publish_with_no_subs(pub, topic, data):
    pub.publish(topic, data)  # nothing should happen


def test_unsub_not_subscribed(pub, topic):
    pub.unsubscribe(topic)


def test_receive_all(pub, service, topic):
    sub = service.get_client()
    sub.subscribe(topic)

    sent = [f"MSG{i}" for i in range(100)]
    for msg in sent:
        pub.publish(topic, msg)

    time.sleep(.1)

    received = sub.get_all()
    assert received == sent


def test_threaded_client(service, pub, topic, data):
    sub = service.get_client()
    sub.subscribe(topic)
    thr = Thread(target=sub.get_single, args=(.1,))
    thr.start()
    pub.publish(topic, data)
    thr.join(.5)
