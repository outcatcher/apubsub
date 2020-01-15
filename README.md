# apubsub

Simple, single-purpose message service implementation.

[![Build Status](https://travis-ci.org/outcatcher/apubsub.svg?branch=master)](https://travis-ci.org/outcatcher/apubsub)
[![Coverage](https://codecov.io/gh/outcatcher/apubsub/branch/master/graph/badge.svg)](https://codecov.io/gh/outcatcher/apubsub)
[![PyPI version](https://img.shields.io/pypi/v/apubsub.svg)](https://pypi.org/project/apubsub/)


*Note that service is started in stand-alone process, so start it as early as possible to minimize resource pickling*

### Installation

_Python versin 3.7+ required_

Just install it with pip: `pip install apubsub`

### Usage

The most simple usage is to subscribe to topic and receive single message:

```python
from apubsub import Service

service = Service()
service.start()

pub = service.get_client()
sub = service.get_client()  # every client can perform both pub and sub roles

sub.subscribe('topic')

pub.publish('topic', 'some data')  # publish put data to subscribed Client queue

pub.publish('topic', 'some more data')
pub.publish('topic', 'and more')

data = sub.get_single(timeout=0.1)  # 'some data'

data = sub.get_all()  # ['some more data', 'and more']

```

Also, `Client` provides `start_receiving` async generator for receiving messages on-demand.
It will wait for new messages until interrupted by `stop_receiving` call
