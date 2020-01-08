import random
import string
import sys
import time
from multiprocessing import Process
from threading import Thread
from unittest import TestCase, skipIf

from psub.client import Client
from psub.server import Service


def _rand_str():
    r_string = "".join(random.choice(string.printable) for _ in range(10))
    return r_string


def _client_exists(srv, client):
    assert srv.client_registered(client)


class TestService(TestCase):
    service: Service

    @classmethod
    def setUpClass(cls):
        cls.service = Service(False)
        cls.service.start()

    @classmethod
    def tearDownClass(cls):
        cls.service.stop()


class TestClientRegistration(TestService):

    def test_start(self):
        pass

    def test_client(self):
        cli1 = self.service.get_client()
        _client_exists(self.service, cli1)

    def test_threaded_client(self):
        cli1 = self.service.get_client()
        cli2 = self.service.get_client()
        t1 = Thread(target=_client_exists, args=(self.service, cli1))
        t2 = Thread(target=_client_exists, args=(self.service, cli2))
        t1.start()
        t2.start()
        t1.join(2)
        t2.join(2)

    @skipIf(sys.platform == "win32", reason="Subprocessed clients are not working properly on windows")
    def test_subprocessed_client(self):
        cli1 = self.service.get_client()
        cli2 = self.service.get_client()
        p1 = Process(target=_client_exists, args=(self.service, cli1))
        p2 = Process(target=_client_exists, args=(self.service, cli2))
        p1.start()
        p2.start()
        p1.join(2)
        p2.join(2)
        self.assertEquals((p1.exitcode, p2.exitcode), (0, 0))


class TestSubscription(TestService):

    def test_multiple_subs(self):
        sub1 = self.service.get_client()
        sub2 = self.service.get_client()
        pub = self.service.get_client()
        topic = "TOPIC"

        sub1.subscribe(topic)
        sub2.subscribe(topic)
        data = _rand_str()

        pub.publish(topic, data)

        received = sub1.receive_single()
        self.assertEqual(received, data)

        received = sub2.receive_single()
        self.assertEqual(received, data)

    def test_receive_generator(self):
        count = random.randrange(5)
        topic = _rand_str()
        pub = self.service.get_client()
        sub = self.service.get_client()
        sub.subscribe(topic)

        generated = [_rand_str() for _ in range(count)]
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

        data = _rand_str()
        pub.publish(topic, data)
        s1_data = [sub1.receive_single()]
        s2_data = [sub2.receive_single()]
        sub2.unsubscribe(topic)
        data2 = _rand_str()
        pub.publish(topic, data2)

        s1_data.append(sub1.receive_single())
        s2_data.append(sub2.receive_single())

        self.assertListEqual(s1_data, [data, data2])
        self.assertListEqual(s2_data, [data, None])

    def test_control_data_mix(self):
        sub = self.service.get_client()
        pub = self.service.get_client()

        topic = _rand_str()
        sub.subscribe(topic)

        sent = _rand_str()
        pub.publish(topic, sent)
        sub.subscribe("topic2")
        time.sleep(0.1)

        received = sub.receive_single()
        self.assertEqual(received, sent)

    def test_receive_sub_separated_data(self):
        sub = self.service.get_client()
        pub = self.service.get_client()

        topic = _rand_str()
        sub.subscribe(topic)

        sent = "this is data, and, \x00no split required,"
        pub.publish(topic, sent)
        time.sleep(0.1)
        received = sub.receive_single()
        self.assertEqual(received, sent)


def _receive_several(sub: Client, storage: list):
    for message in sub.start_receiving():
        storage.append(message)
