import gevent

from common import *


class TestQueue(BaseTestCase):
    """imitate message queue

    can be used in these patterns:
    1. distributed tasks/jobs processing by multiple workers
    2. ...
    """

    def test_2_workers_grab_1_job(self):
        self.assertEqual(self.post(['foo']), [1])
        g1=gevent.spawn(self.pull)
        g2=gevent.spawn(self.pull)
        ready_greenlets=gevent.joinall([g1, g2], timeout=0.2)
        self.assertEqual(len(ready_greenlets), 1)
        g=ready_greenlets[0]
        messages=g.get()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        if g==g1:
            g=g2
        else:
            g=g1
        gevent.kill(g)

    def test_2_workers_wait_1_job(self):
        g1=gevent.spawn(self.pull)
        g2=gevent.spawn(self.pull)
        gevent.idle()
        self.assertEqual(self.post(['foo']), [1])
        ready_greenlets=gevent.joinall([g1, g2], timeout=0.2)
        self.assertEqual(len(ready_greenlets), 1)
        g=ready_greenlets[0]
        messages=g.get()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        if g==g1:
            g=g2
        else:
            g=g1
        gevent.kill(g)

    def test_2_workers_wait_2_jobs(self):
        g1=gevent.spawn(self.pull)
        g2=gevent.spawn(self.pull)
        # yield current greenlet
        # there is no gurantee that nginx will process reqeusts g1 and g2
        # in the order they are sent, so sometimes g1 can not get the previous message
        gevent.idle()
        self.assertEqual(self.post(['foo', 'bar']), [1, 2])
        gevent.joinall([g1, g2])
        messages=g1.get()+g2.get()
        ids = [message['id'] for message in messages]
        self.assertItemsEqual(ids, [1, 2])

    def test_2_workers_get_2_jobs(self):
        g1=gevent.spawn(self.pull)
        g2=gevent.spawn(self.pull)
        gevent.idle()
        self.assertEqual(self.post(['foo']), [1])
        ready_greenlets=gevent.wait([g1, g2], count=1)
        g=ready_greenlets[0]
        messages=g.get()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])

        self.assertEqual(self.post(['bar']), [2])
        if g==g1:
            g=g2
        else:
            g=g1
        messages=g.get()
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])







