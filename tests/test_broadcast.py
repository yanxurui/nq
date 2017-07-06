import gevent

from common import *


class TestBroadcast(BaseTestCase):
    """pub/sub pattern
    
    """
    def test_2_subscribers_take_1_message(self):
        self.assertEqual(self.post(['foo']), [1])
        g1=gevent.spawn(self.pull)
        g2=gevent.spawn(self.pull, receiver='receiver2')
        gevent.joinall([g1, g2], timeout=0.2)
        messages=g1.value
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        messages=g2.value
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])

    def test_2_subscribers_wait_1_message(self):
        g1=gevent.spawn(self.pull)
        g2=gevent.spawn(self.pull, receiver='receiver2')
        gevent.idle()
        self.assertEqual(self.post(['foo']), [1])
        gevent.joinall([g1, g2], timeout=0.2)
        messages=g1.value
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        messages=g2.value
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])

    def test_3_subscribers_take_2_messages(self):
        self.assertEqual(self.post(['foo', 'bar', 'baz']), [1, 2, 3])
        g1=gevent.spawn(self.pull, start=2, max_count=2)
        g2=gevent.spawn(self.pull, start=2, max_count=2, receiver='receiver2')
        g3=gevent.spawn(self.pull, start=2, max_count=2, receiver='receiver3')
        gevent.joinall([g1, g2, g3], timeout=0.2)
        
        messages1=g1.value
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages1[0])
        self.assertDictContainsSubset({'id':3,'message':'baz'}, messages1[1])
        messages2=g2.value
        messages3=g3.value
        self.assertEqual(messages2, messages1)
        self.assertEqual(messages3, messages1)
