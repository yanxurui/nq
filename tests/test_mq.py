import json
from unittest import TestCase
import time

from mq import queue

class TestMQ(TestCase):
    qname = 'mymq'
    sender = 'foo'
    def tearDown(self):
        queue.delete(self.qname)

    def test_create(self):
        q1 = queue(self.qname)
        q2 = queue(self.qname)
        self.assertEqual(q1, q2)

    def test_one_msg(self):
        q = queue('mymq')
        
        msg = 'hello world'
        mid = q.put(msg)
        self.assertEqual(mid, '%s_%d' % (self.qname, 0))
        self.assertEqual(len(q), 1)
        
        m = q.get(0)
        self.assertEqual(m, msg)

    def test_multi_msg_one_by_one(self):
        q = queue('mymq', fragment_size=10)
        msgs = []
        for i in range(15):
            msg = 'msg %d' % i
            msgs.append(msg)
            mid = q.put(msg)
            self.assertEqual(mid, '%s_%d' % (self.qname, i))
            self.assertEqual(len(q), i+1)
        self.assertEqual(q.get(0), msgs[0])
        self.assertEqual(q.get(10), msgs[10])
        self.assertEqual(q.get(14), msgs[14])

    def test_multi_msg(self):
        q = queue('mymq', fragment_size=10)
        msgs = []
        for i in range(23):
            msgs.append('msg %d' % i)
        mids = q.put(msgs[:5])
        self.assertEqual(mids, ['%s_%d' % (self.qname, i) for i in range(5)])
        self.assertEqual(len(q), 5)

        mids = q.put(msgs[5:15])
        self.assertEqual(mids, ['%s_%d' % (self.qname, i) for i in range(5, 15)])
        self.assertEqual(len(q), 15)

        mids = q.put(msgs[15:20])
        self.assertEqual(mids, ['%s_%d' % (self.qname, i) for i in range(15, 20)])
        self.assertEqual(len(q), 20)

        mids = q.put(msgs[20:23])
        self.assertEqual(mids, ['%s_%d' % (self.qname, i) for i in range(20, 23)])
        self.assertEqual(len(q), 23)

        self.assertEqual(q.get(0), msgs[0])
        self.assertEqual(q.get(10), msgs[10])
        self.assertEqual(q.get(14), msgs[14])
        self.assertEqual(q.get(22), msgs[22])

        self.assertEqual(q.get(0, limit=5), msgs[:5])
        self.assertEqual(q.get(5, limit=10), msgs[5:15])
        self.assertEqual(q.get(0, limit=23), msgs)

