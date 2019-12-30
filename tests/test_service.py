from multiprocessing import Process
from threading import Thread
from unittest import TestCase

from psub.server import Service


def _client_exists(srv, client):
    assert srv.client_registered(client)


class Tests(TestCase):

    def setUp(self):
        self.service = Service()
        self.service.start()

    def tearDown(self):
        self.service.stop()

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

    def test_subprocessed_client(self):
        cli1 = self.service.get_client()
        cli2 = self.service.get_client()
        p1 = Process(target=_client_exists, args=(self.service, cli1))
        p2 = Process(target=_client_exists, args=(self.service, cli2))
        p1.start()
        p2.start()
        p1.join(2)
        p2.join(2)
        assert p1.exitcode == 0
        assert p2.exitcode == 0
