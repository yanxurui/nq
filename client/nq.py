import sys
import json
import logging
import signal
import functools
import traceback
from multiprocessing import Process, Event
from collections import defaultdict

import requests
import gevent
from gevent import sleep
from gevent import queue
from gevent import monkey
from gevent import pool

logger = logging.getLogger(__name__)


class Base(object):
    def __init__(self, name, processes, server='http://127.0.0.1:8001', timeout=60):
        self.name = name
        self.processes = processes
        self.server = server
        self.timeout = timeout
        self.event = Event()

        self.queues_func = {}
        self.queues_args = {}

    def handler(self, queue, **kargs):
        """a decorator to specify message handler
        
        [description]
        
        Arguments:
            queue {str} -- [queue name]
            **kargs {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """
        def decorator(func):
            self.queues_func[queue] = func
            self.queues_args[queue] = kargs
            # @functools.wraps(func)
            # def wrapper(*args, **kw):
            #     return func(*args, **kw)
            # return wrapper
            return func
        return decorator

class ProcessPool(Base):
    """do CPU bound tasks in multiple processes

    Extends:
        Base
    """
    def _sigterm_handler(self, _signo, _stack_frame):
        print('got SIGTERM')
        self.event.set()
        sys.exit(0)

    def _worker(self, w_id):
        logger.info('start worker %d' % w_id)
        session = requests.session()
        data = {
                "receiver": self.name,
                "timeout": self.timeout,
                "queues": self.queues_args,
            }
        results = defaultdict(dict)
        while not self.event.is_set():
            t = 1
            data["results"] = results
            logger.info('pull messages')
            try:
                r = session.post(self.server+'/pull', json=data)
            except requests.ConnectionError:
                logger.error(traceback.format_exc())
                t = t * 2
                sleep(t)
                continue
            results.clear()
            if r.status_code == 200:
                messages = r.json()['messages']
                for queue, msgs in messages.items():
                    for msg in msgs:
                        msg_id = msg['id']
                        logger.info('worker %d got message %d from %s' % (w_id, msg_id, queue))
                        try:
                            rst = self.queues_func[queue](msg)
                            results[queue][msg_id] = rst
                        except KeyboardInterrupt:
                            raise
                        except:
                            logger.error(traceback.format_exc())
            elif r.status_code == 204:
                continue # no message yet
            else:
                logger.error('%d, %s' % (r.status_code, r.text))
        logger.warning('worker %d exit' % w_id)

    def start(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)
        workers = []
        for i in range(1, self.processes+1):
            worker = Process(target=self._worker, args=(i,))
            # worker.daemon = True
            worker.start()
            workers.append(worker)
        for worker in workers:
            worker.join()


class GreenletPool(Base):
    """do IO bound tasks in gevent pool
    
    [description]
    
    Extends:
        Base
    """
    def __init__(self, *args, **kargs):
        Base.__init__(self, *args, **kargs)
        self.rst_queue = gevent.queue.Queue()
        self.pool = gevent.pool.Pool(self.processes+2)

    def handler(self, queue, **kargs):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(queue, msg):
                msg_id = msg['id']
                logger.info('got message %d from %s' % (msg_id, queue))
                try:
                    rst = func(msg)
                    self.rst_queue.put((queue, msg_id, str(rst)))
                    return rst
                except KeyboardInterrupt:
                    raise
                except:
                    logger.error(traceback.format_exc())
            self.queues_func[queue] = wrapper
            self.queues_args[queue] = kargs
            return wrapper
        return decorator

    def _pull(self):
        session = requests.session()
        data = json.dumps({
                "receiver": self.name,
                "timeout": self.timeout,
                "queues": self.queues_args
            })
        while True:
            logger.info('pull messages')
            t = 1
            try:
                r = session.post(self.server+'/pull', data=data)
            except requests.ConnectionError:
                logger.error(traceback.format_exc())
                t = t * 2
                sleep(t)
                continue
            if r.status_code == 200:
                messages = r.json()['messages']
                for queue, msgs in messages.items():
                    for msg in msgs:
                        self.pool.spawn(self.queues_func[queue], queue, msg)
            elif r.status_code == 204:
                logger.info('no message available')
                continue
            else:
                logger.error('%d, %s' % (r.status_code, r.text))

    def _report(self):
        session = requests.session()
        while True:
            results = defaultdict(dict)
            t = 1
            queue, msg_id, rst = self.rst_queue.get()
            logger.info('report results')
            while True:
                results[queue][msg_id] = rst
                try:
                    queue, msg_id, rst = self.rst_queue.get_nowait()
                except gevent.queue.Empty as e:
                    break
            try:
                r = session.post(self.server+'/pull', json={
                    "receiver": self.name,
                    "results": results
                })
            except requests.ConnectionError:
                logger.error(traceback.format_exc())
                t = t * 2
                sleep(t)
                continue
            if not r.status_code == 200:
                logger.error('%d, %s' % (r.status_code, r.text))

    def start(self):
        gevent.monkey.patch_all()
        self.pool.spawn(self._pull)
        self.pool.spawn(self._report)
        self.pool.join()

