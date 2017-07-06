import gevent

from common import *


class TestPull(BaseTestCase):
    def test_pull_messages(self):
        self.assertEqual(self.post(['foo', 'bar', 'baz']), [1, 2, 3])
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        
        messages=self.pull(max_count=3)
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])
        self.assertDictContainsSubset({'id':3,'message':'baz'}, messages[1])
        self.assertNotIn('created_time', messages[0])

    def test_pull_start(self):
        self.assertEqual(self.post(['foo', 'bar', 'baz']), [1, 2, 3])
        messages=self.pull(start=2)
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])
        
        messages=self.pull()
        self.assertDictContainsSubset({'id':3,'message':'baz'}, messages[1])

    def test_pull_wait(self):
        # the queue is still empty
        g1=gevent.spawn(self.pull)
        gevent.idle()
        self.assertEqual(self.post(['foo']), [1])
        messages=g1.get()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])

        # there is no available message yet
        g2=gevent.spawn(self.pull, max_count=2)
        gevent.idle()
        self.assertEqual(self.post(['bar']), [2])
        messages=g2.get()
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])

        g3=gevent.spawn(self.pull)
        self.assertEqual(self.post(['baz', 'qux']), [3, 4])
        messages=g3.get()
        self.assertDictContainsSubset({'id':3,'message':'baz'}, messages[0])

    def test_pull_last(self):
        self.assertEqual(self.post(['foo', 'bar']), [1, 2])
        # there are 2 messages avaiable, but only receive the last one
        messages=self.pull(start=-1)
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])
        
        self.assertEqual(self.post(['fooooo', 'baaaaaa']), [3, 4])
        messages=self.pull(start=-1)
        self.assertDictContainsSubset({'id':4,'message':'baaaaaa'}, messages[0])

        # there is no message available, wait for next message
        g1=gevent.spawn(self.pull, start=-1)
        gevent.idle()
        self.assertEqual(self.post(['foo', 'baz']), [5, 6])
        messages=g1.get()
        # 2 more messages available, only receive the last one
        self.assertDictContainsSubset({'id':6,'message':'baz'}, messages[0])

    def test_pull_ignore_existing(self):
        self.assertEqual(self.post(['foo']), [1])

        # ignore existing messages, wait for new message
        g1=gevent.spawn(self.pull, start=0, max_count=3)
        gevent.idle()
        self.assertEqual(self.post(['bar', 'baz']), [2,3])
        messages = g1.get()
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])
        self.assertDictContainsSubset({'id':3,'message':'baz'}, messages[1])

    def test_pull_2_queues(self):
        self.assertEqual(self.post(['foo', 'bar'], queue='queue1'), [1, 2])
        self.assertEqual(self.post(['baz', 'qux'], queue='queue2'), [1, 2])
        data = {
            "receiver": "receiver1",
            "queues": {
              "queue1": {
                "start": 1,
                "max": 3
              },
              "queue2": {
                "start": 1,
                "max": 3
              }
            }
        }
        r = s.post('/pull', json=data)
        self.assertEqual(r.status_code, 200)
        messages = r.json()['messages']
        print(messages)
        messages1 = messages['queue1']
        self.assertEqual(len(messages1), 2)
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages1[0])
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages1[1])

        messages2 = messages['queue2']
        self.assertEqual(len(messages2), 2)
        self.assertDictContainsSubset({'id':1,'message':'baz'}, messages2[0])
        self.assertDictContainsSubset({'id':2,'message':'qux'}, messages2[1])
        
        # wait for new messages
        g=gevent.spawn(s.post, '/pull', json=data)
        gevent.idle()

        self.assertEqual(self.post(['fooo', 'baaa'], queue='queue1'), [3, 4])
        self.assertEqual(self.post(['bazz'], queue='queue2'), [3])

        r = g.get()
        messages = r.json()['messages']
        messages1 = messages['queue1']
        self.assertEqual(len(messages1), 2)
        self.assertDictContainsSubset({'id':3,'message':'fooo'}, messages1[0])
        self.assertDictContainsSubset({'id':4,'message':'baaa'}, messages1[1])
        # in this special case, only return queue1's new message
        self.assertNotIn('queue2', messages)

    def test_pull_timeout(self):
        start = time()
        data = {
            "receiver": "receiver1",
            "queues": {
              "queue1": {}
            },
            "timeout": 0.5
        }
        r = s.post('/pull', json=data)
        elapsed = time() - start
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json(), {
            "queue1": 0
        })
        self.assertAlmostEqual(elapsed, 0.5, delta=0.1)


