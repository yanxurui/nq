import gevent

from common import *


class TestPublish(BaseTestCase):
    def test_publish_1_message_twice(self):
        self.assertEqual(self.post(['foo']), [1])
        self.assertEqual(self.post(['bar']), [2])

    def test_publish_multiple_messages(self):
        self.assertEqual(self.post(['foo', 'bar']), [1, 2])
        self.assertEqual(self.post(['baz']), [3])
        self.assertEqual(self.post(['fooooo', 'bazzzzz']), [4, 5])

    def test_publish_2_senders(self):
        g1=gevent.spawn(self.post, ['foo'])
        g2=gevent.spawn(self.post, ['bar', 'baz'], sender='sender2')
        gevent.joinall([g1, g2])
        self.assertItemsEqual(g1.get()+g2.get(), [1, 2, 3])

    def test_publish_2_queues(self):
        data = {
            "sender": "sender1",
            "messages": {
              "queue1": ['foo'],
              "queue2": ['bar', 'baz']
            }
        }
        r = s.post('/post', json=data)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json(), {
            'queue1': [1],
            'queue2': [1, 2]
        })
    
    def test_publish_empty(self):
        data = {
            "sender": "sender1",
            "messages": {
              "queue1": [],
            }
        }
        r = s.post('/post', json=data)
        self.assertEqual(r.status_code, 400)