import random
import socket
import string
import time
from threading import Thread
from unittest import TestCase

from psub.client import Client
from psub.server import Service


def _rand_str(size=10, charset=string.ascii_letters):
    r_string = "".join(random.choice(charset) for _ in range(size))
    return r_string


def _client_exists(srv, client):
    assert srv.client_registered(client)


class TestService(TestCase):
    service: Service

    @classmethod
    def setUpClass(cls):
        cls.service = Service()
        cls.service.start()
        sock = socket.socket(socket.AF_INET)
        sock.settimeout(3)
        time.sleep(0.5)

        try:
            sock.connect(("localhost", cls.service.port))
        finally:
            sock.close()

    @classmethod
    def tearDownClass(cls):
        cls.service.stop()


class TestSubscription(TestService):

    def test_multiple_subs(self):
        sub1 = self.service.get_client()
        sub2 = self.service.get_client()
        pub = self.service.get_client()
        topic = "TOPIC"

        sub1.subscribe(topic)
        sub2.subscribe(topic)
        data = _rand_str(200, string.printable)

        pub.publish(topic, data)

        received = sub1.get_single()
        self.assertEqual(received, data)

        received = sub2.get_single()
        self.assertEqual(received, data)

    def test_receive_generator(self):
        count = random.randrange(5)
        topic = _rand_str()
        pub = self.service.get_client()
        sub = self.service.get_client()
        sub.subscribe(topic)

        generated = [_rand_str(200, string.printable) for _ in range(count)]
        received = []
        thr = Thread(target=_receive_several, args=(sub, received))
        thr.start()
        for data in generated:
            pub.publish(topic, data)
        time.sleep(0.5)
        sub.stop_receiving()
        thr.join()
        self.assertListEqual(received, generated)

    def test_unsubscribe(self):
        sub1 = self.service.get_client()
        sub2 = self.service.get_client()
        pub = self.service.get_client()
        topic = "TOPIC"
        sub1.subscribe(topic)
        sub2.subscribe(topic)

        data = _rand_str(200, string.printable)
        pub.publish(topic, data)
        s1_data = [sub1.get_single()]
        s2_data = [sub2.get_single()]
        sub2.unsubscribe(topic)
        data2 = _rand_str(200, string.printable)
        pub.publish(topic, data2)

        s1_data.append(sub1.get_single())
        s2_data.append(sub2.get_single())

        self.assertListEqual(s1_data, [data, data2])
        self.assertListEqual(s2_data, [data, None])

    def test_control_data_mix(self):
        sub = self.service.get_client()
        pub = self.service.get_client()

        topic = _rand_str()
        sub.subscribe(topic)

        sent = _rand_str(200, string.printable)
        pub.publish(topic, sent)
        sub.subscribe("topic2")
        time.sleep(0.1)

        received = sub.get_single()
        self.assertEqual(received, sent)

    def test_receive_big_data(self):
        sub = self.service.get_client()
        pub = self.service.get_client()

        topic = _rand_str()
        sub.subscribe(topic)

        sent = _rand_str(2000)
        pub.publish(topic, sent)
        time.sleep(0.1)
        received = sub.get_single()
        self.assertEqual(received, sent)


def _receive_several(sub: Client, storage: list):
    for message in sub.start_receiving():
        storage.append(message)
