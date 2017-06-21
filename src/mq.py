import json
import os
import shutil
from os.path import join
import time

import conf



class queue(object):
    """docstring for MQ"""
    _instances = {}
    def __new__(cls, name, **options):
        """create only one instance for the same queue
        
        flyweight pattern
        
        Arguments:
            name {[type]} -- [description]
            **options {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """
        if name in queue._instances:
            return queue._instances[name]
        else:
            instance = super(queue, cls).__new__(cls, name, **options)
            queue._instances[name] = instance
            return instance

    def __init__(self, name, **options):
        self.name = name
        self._queue = []
        self._offset = self._create_if_not_exists(**options)
        assert self._offset%self._OPT['fragment_size'] == 0
        self._file = self._open(self._offset, mode='a')
        
        self._waiting_receivers = []

    def _create_if_not_exists(self, fragment_size=1000):
        directory = join(conf.DATA_PATH, self.name)
        if not os.path.isdir(directory):
            os.makedirs(join(directory, 'messages'))
            os.mkdir(join(directory, 'results'))
            os.mkdir(join(directory, 'deleted'))
            self._OPT = {
                'fragment_size': fragment_size
            }
            with open(join(directory, 'META'), 'w') as f:
                f.write(json.dumps(self._OPT))
            return 0
        else:
            with open(join(directory, 'META')) as f:
                self._OPT = json.loads(f.read())
            msg_dir = join(directory, 'messages')
            offset = max(map(int, os.listdir(msg_dir)))
            with open(join(directory, 'messages', str(offset))) as f:
                self._queue = f.read().splitlines()
            return offset

    def _open(self, start, mode='r'):
        directory = join(conf.DATA_PATH, self.name, 'messages')
        filename = str(start)
        # if the file does not exist:
        #   `r`: an exceptioin occurs
        #   `a`: a new file will be created automatically
        return open(join(directory, filename), mode)

    def __len__(self):
        return self._offset + len(self._queue)

    def put(self, msgs):
        if type(msgs) == str:
            mids = '%s_%d' % (self.name, self.__len__())
            self._queue.append(msgs)
            # todo: new line in msg
            self._file.write(msgs + '\n')
            if len(self._queue) == self._OPT['fragment_size']:
                self._file.close()
                self._offset += self._OPT['fragment_size']
                self._file = self._open(self._offset, mode='a')
                self._queue = []
        else: # list
            assert len(msgs) <= self._OPT['fragment_size']
            mids = ['%s_%d' % (self.name, self.__len__()+i) for i in range(len(msgs))]
            if len(self._queue) + len(msgs) >= self._OPT['fragment_size']:
                l = self._OPT['fragment_size'] - len(self._queue)
                self._file.write('\n'.join(msgs[:l]) + '\n')
                self._file.close()
                self._offset += self._OPT['fragment_size']
                self._file = self._open(self._offset, mode='a')
                self._queue = msgs[l:]
                self._file.write('\n'.join(self._queue) + '\n')
            else:
                self._queue.extend(msgs)
                self._file.write('\n'.join(msgs) + '\n')
        return mids

    def get(self, offset, limit=1):
        assert offset >= 0 and offset < self.__len__()
        assert limit > 0
        end = offset + limit
        if end > self.__len__():
            end = self.__len__()
            limit = end - offset
        if offset >= self._offset:
            if limit == 1:
                return self._queue[offset - self._offset]
            return self._queue[offset - self._offset : end - self._offset]
        
        start = offset/self._OPT['fragment_size']*self._OPT['fragment_size']
        assert start <= offset < end
        msgs = []
        while True:
            file = self._open(start)
            for msg in file:
                if start == end:
                    break
                if start >= offset:
                    msgs.append(msg[:-1])
                start += 1   
            file.close()
            if start == end:
                if len(msgs) == 1:
                    return msgs[0]
                return msgs
            assert start % self._OPT['fragment_size'] == 0
            if start == self._offset:
                msgs.extend(self._queue[:end-start])
                return msgs

    @classmethod
    def delete(cls, name):
        if name in cls._instances:
            del cls._instances[name]
        shutil.rmtree(join(conf.DATA_PATH, name))



