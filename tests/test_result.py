import gevent

from common import *


class TestResult(BaseTestCase):
    def test_ack_1_message(self):
        self.assertEqual(self.post(['foo']), [1])
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        data = {
            'receiver': 'receiver1',
            'results': {
                'queue1': {
                    '1':'done'
                }
            }
        }
        r=s.post('/pull', json=data)
        self.assertEqual(r.status_code, 200)
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'status':'finished',
                'result':'done'
            }
        )

    def test_ack_2_messages(self):
        self.assertEqual(self.post(['foo', 'bar', 'baz']), [1, 2, 3])
        messages=self.pull(max_count=2)
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[1])
        data = {
            'receiver': 'receiver1',
            'results': {
                'queue1': {
                    '1':'ok',
                    '2':'received'
                }
            }
        }
        r=s.post('/pull', json=data)
        self.assertEqual(r.status_code, 200)
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'result':'ok'
            }
        )
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':2,
                'receiver':'receiver1',
                'result':'received'
            }
        )

    def test_pull_and_ack(self):
        """acknowledge result and pull new message at the same time
        
        [description]
        """
        self.assertEqual(self.post(['foo', 'bar']), [1, 2])
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        data = {
            'receiver': 'receiver1',
            'queues': {
              'queue1': {}
            },
            'results': {
                'queue1': {
                    '1':'received',
                }
            }
        }
        r=s.post('/pull', json=data)
        self.assertEqual(r.status_code, 200)
        messages = r.json()['messages']['queue1']
        self.assertLessEqual(len(messages), 1)
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])

        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'result':'received'
            }
        )
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':2,
                'receiver':'receiver1',
                'status':'pending',
            }
        )




