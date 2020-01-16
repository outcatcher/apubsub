# APubSub

[![Build Status](https://travis-ci.org/outcatcher/apubsub.svg?branch=master)](https://travis-ci.org/outcatcher/apubsub)
[![Coverage](https://codecov.io/gh/outcatcher/apubsub/branch/master/graph/badge.svg)](https://codecov.io/gh/outcatcher/apubsub)
[![PyPI version](https://img.shields.io/pypi/v/apubsub.svg)](https://pypi.org/project/apubsub/)
![GitHub](https://img.shields.io/github/license/outcatcher/apubsub)

Simple, single-purpose message service implementation.

### Installation

_Python versin 3.7+ required_

Just install it with pip: `pip install apubsub`

### Usage


```python
from apubsub import Service

service = Service()

# Note that service is started in stand-alone process
# so start it as early as possible to minimize resource pickling*
service.start()

class Klass:

    def __init__(self):
        self.sub = service.get_client()
        await self.sub.start_consuming()  # subscriber should be started
        
        self.pub = service.get_client()  # if used only as publisher, it is not required

    async def do_smth(self):
        data = await self.sub.get(.1)  # fetch received data with timeout
        if data is None:
            print("No data received by subscriber")
            return
        print(data)
    
    async def do_smth_else(self):
        msg = "some string data"
        await self.pub.publish("topic", msg)

    async def use_iter_get(self):
        async for data in self.sub.get_iter():
            print(f"Data received: {data}")

```

_Check out more examples in tests_

