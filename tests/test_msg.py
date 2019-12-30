import time
from threading import Thread

from psub import Service


def _p2_rcv():
    _updates = service.subscribe("test")
    _data = _updates.get()
    print(_data)


if __name__ == '__main__':
    service = Service()
    # p2 = Process(target=_p2_rcv, args=(service,))

    service.start()
    updates = service.subscribe("test")
    time.sleep(0.5)
    t2 = Thread(target=_p2_rcv)
    t2.start()
    t2.join()

    service.publish("test", "test me")
    # p2.start()
    # t2.start()
    # p2.join()
    # t2.join()

    data = updates.get()
    print("Process 1: ", data)
    service.stop()
    assert service._srv_process.exitcode == 0
