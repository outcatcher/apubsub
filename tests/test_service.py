import asyncio
import string
from typing import List

import pytest

from apubsub import Service
from apubsub.client import Client, ClientError
from apubsub.connection_wrapper import receive
from apubsub.protocol import CMD_PUB, MAX_PACKET_SIZE, MaxSizeOverflow
from tests.helpers import rand_str, started_client

pytestmark = pytest.mark.asyncio


async def test_get_port(pub: Client, service):
    port = await pub.get_port()
    assert isinstance(port, int)
    assert service.port + 100 >= port >= service.port + 1


async def test_simple_publish(pub: Client, sub: Client, topic, data, service):
    await sub.subscribe(topic)
    await pub.publish(topic, data)
    recv = await sub.get(.1)
    assert recv == data


async def test_multiple_subs(pub: Client, topic, data, service):
    sub_coros = [started_client(service) for _ in range(10)]
    subs: List[Client] = await asyncio.gather(*sub_coros)
    await asyncio.wait([sub.subscribe(topic) for sub in subs])
    await pub.publish(topic, data)
    all_received = await asyncio.gather(*[sub.get(.1) for sub in subs])
    assert all_received == [data] * len(subs)


async def test_receive_generator(service, pub: Client, data, topic):
    count = 5
    sub = await started_client(service)
    await sub.subscribe(topic)
    generated_data = [rand_str(200, string.printable) for _ in range(count)]
    await asyncio.wait([
        pub.publish(topic, _data) for _data in generated_data
    ])

    loop = asyncio.get_running_loop()
    loop.call_later(.3, sub.stop_getting)

    async for _data in sub.get_iter():
        pos = generated_data.index(_data)
        generated_data.pop(pos)

    assert sub._data_queue.qsize() == 0
    assert not generated_data, f"Not all data consumed: {generated_data}"


async def test_receive_generator_remains(pub: Client, sub: Client, topic, data):
    await sub.subscribe(topic)
    await pub.publish(topic, data)
    await pub.publish(topic, data)

    async for received in sub.get_iter():
        assert received == data
        break

    assert sub._data_queue.qsize() == 1


async def test_unsubscribe(service, pub: Client, topic, data):
    topic2 = "topic2"
    data2 = "Second data"
    subs: List[Client] = await asyncio.gather(*[started_client(service) for _ in range(2)])

    await asyncio.wait([
        asyncio.gather(
            sub.subscribe(topic),
            sub.subscribe(topic2),
        )
        for sub in subs
    ])
    special_sub = subs.pop(0)
    await special_sub.unsubscribe(topic2)

    await asyncio.wait([
        pub.publish(topic, data),
        pub.publish(topic2, data2),
    ])

    received_special = [
        await special_sub.get(.1),
        await special_sub.get(.1),
    ]

    assert received_special == [data, None]

    expected = sorted([data, data2])
    received = [
        sorted([await sub.get(.1), await sub.get(.1)])
        for sub in subs
    ]

    assert sorted(received) == [expected] * len(subs)


async def test_receive_big_data(service, pub: Client, topic):
    sub = await started_client(service)
    await sub.subscribe(topic)
    sent = rand_str(200_000, string.printable)
    await pub.publish(topic, sent)
    received = await sub.get(.1)
    assert received == sent


async def test_unknown_command(service):
    client = service.get_client()
    with pytest.raises(ClientError):
        await client.send_command("UNKNOWN", "", "")


async def test_too_big(pub: Client):
    with pytest.raises(MaxSizeOverflow):
        await pub.publish("topic", "A" * MAX_PACKET_SIZE + "A")


async def test_not_allowed_topic(pub: Client):
    with pytest.raises(TypeError):
        await pub.subscribe("topic:1")


async def test_bytes_command(pub: Client):
    await pub.subscribe("topic")
    with pytest.raises(TypeError):
        await pub.send_command(CMD_PUB, b"topic", None)


async def test_publish_with_no_subs(pub: Client, topic, data):
    await pub.publish(topic, data)


async def test_unsub_not_subscribed(pub: Client, topic):
    await pub.unsubscribe(topic)


async def test_receive_all(pub: Client, service, topic):
    sub = await started_client(service)
    await sub.subscribe(topic)

    sent = [f"MSG{i}" for i in range(100)]
    await asyncio.wait([
        pub.publish(topic, msg) for msg in sent
    ])

    await asyncio.sleep(.2)

    received = sub.get_all()
    assert sorted(received) == sorted(sent)


async def test_publish_not_started(service, topic, data, sub: Client):
    pub = service.get_client()
    await sub.subscribe(topic)

    await pub.publish(topic, data)
    received = await sub.get(.1)
    assert received == data


async def test_receive_not_started(service, topic, data, pub: Client):
    sub = service.get_client()
    await sub.subscribe(topic)

    await pub.publish(topic, data)

    with pytest.raises(ValueError):
        await sub.get(.1)


async def test_send_invalid_message(service, topic):
    reader, writer = await asyncio.open_connection("127.0.0.1", service.port)
    message = b"asdijhreawfe23"
    writer.write(message)
    await writer.drain()
    response = await receive(reader)
    assert response == b"Invalid message"


async def test_service_on_another_port(service):
    srv2 = Service()
    assert srv2.address != service.address[0], service.address[1] - 110
