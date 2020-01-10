# apubsub

Simple, single-purpose message service implementation.

*Note that service is started in stand-alone process, so start it as early as possible to minimize resource pickling*

### Usage:
The most simple usage is to subscribe to topic and receive single message:

```python

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
