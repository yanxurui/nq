import gevent

from common import *


class TestRetry(BaseTestCase):
    def test_retry_1_message(self):
        self.assertEqual(self.post(['foo']), [1])
        # pull new messages
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        sleep(0.6)

        # pull again, retry failed message when there is not new message
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'status':'processing',
                'fail_count':1,
                'result':None
            }
        )

        # send ack, wait new message until timeout
        start = time()
        data = {
            'receiver': 'receiver1',
            'results': {
                'queue1': {
                    '1':'done'
                }
            },
            "queues": {
              "queue1": {}
            }
        }
        r=s.post('/pull', json=data)
        self.assertEqual(r.status_code, 204)
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'status':'finished',
                'fail_count':1,
                'result':'done'
            }
        )
        elapsed = time()-start
        self.assertAlmostEqual(elapsed, 0.5, delta=0.1)

    def test_retry_2_message(self):
        self.assertEqual(self.post(['foo']), [1])
        # pull new messages
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        sleep(0.6)

        self.assertEqual(self.post(['bar']), [2])

        # pull again, new message is delivered first
        messages=self.pull()
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])
        
        # retry failed message when there is not new message
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'status':'processing',
                'fail_count':1,
                'result':None
            }
        )

        sleep(0.6)
        # retry again
        messages=self.pull()
        self.assertDictContainsSubset({'id':1,'message':'foo'}, messages[0])
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':1,
                'receiver':'receiver1',
                'status':'failed',
                'fail_count':2,
                'result':None
            }
        )

        # retry next message
        messages=self.pull()
        self.assertDictContainsSubset({'id':2,'message':'bar'}, messages[0])
        self.assertInDatabase(
            'queue1_rst',
            {
                'm_id':2,
                'receiver':'receiver1',
                'status':'processing',
                'fail_count':1,
                'result':None
            }
        )










