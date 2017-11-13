from random import randint
import logging

from gevent import sleep

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

from nq import ProcessPool, GreenletPool

pool = GreenletPool('receiver1', 2, timeout=10)
@pool.handler('queue1')
def queue1(msg):
    sleep(1)
    return 'ok'

@pool.handler('queue2')
def queue2(msg):
    sleep(2)
    return 'ok'

pool.start()
