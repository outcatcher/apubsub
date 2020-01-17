import random
import string

from apubsub import Service


async def started_client(service: Service):
    _clt = service.get_client()
    await _clt.start_consuming()
    return _clt


def rand_str(size=10, charset=string.ascii_letters):
    r_string = "".join(random.choice(charset) for _ in range(size))
    return r_string
