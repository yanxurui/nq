import gevent
from gevent.pool import Pool
from gevent import monkey
monkey.patch_all()
import requests
def listen(receiver):
    data = {
        "receiver": receiver,
        "queues": {
          "queue1": {
            "start": -2, # wait for new messages
            "max": 1
          }
        }
    }
    r = requests.post('http://localhost:8001/pull', json=data)
    assert(r.status_code==204)
    return receiver

pool = Pool(1000)
receivers = []
for i in range(1000):
    receivers.append('receiver%d' % i)
for i in pool.imap_unordered(listen, receivers):
    print(i)
