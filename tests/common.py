#!/usr/bin/env
# coding=utf-8

import os
import unittest
import shutil
import subprocess
import time
from time import time, sleep
import shutil

import requests
import MySQLdb
import gevent
from gevent import monkey

from config import *

monkey.patch_all()


def reload_ngx():
    logs = PREFIX + 'logs/'
    if os.path.isdir(logs):
        os.remove(logs + 'error.log')
        os.remove(logs + 'access.log')
    cmd = [NGX_BIN, '-p', PREFIX]
    if os.path.isfile(PREFIX+'logs/nginx.pid'):
        cmd.extend(['-s', 'reload'])
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as exc:
        print('%d\nstderr: %s' % (exc.returncode, exc.output))
        raise exc
    else:
        if out:
            print('stdout: ' + out)
    sleep(0.1)

def clear_db():
    global db
    db=MySQLdb.connect(
        host=MYSQL['HOST'],
        port=MYSQL['port'],
        user=MYSQL['USRE'],
        passwd=MYSQL['PASSWORD'],
    )
    c=db.cursor()
    c.execute("drop database if exists %s" % MYSQL['DATABASE'])
    c.execute("create database %s character set utf8" % MYSQL['DATABASE'])
    # select database to use
    db=MySQLdb.connect(
        host=MYSQL['HOST'],
        port=MYSQL['port'],
        user=MYSQL['USRE'],
        passwd=MYSQL['PASSWORD'],
        db=MYSQL['DATABASE']
    )
    # important!!!
    # When use innodb, different queries are in the same transaction and the default isolation level is repeatable read
    # so update is invisible
    # https://stackoverflow.com/questions/384228/database-does-not-update-automatically-with-mysql-and-python
    db.autocommit(True)

# set base url for requests
class Session(requests.Session):
    def __init__(self, url_base=HOST, *args, **kwargs):
        super(Session, self).__init__(*args, **kwargs)
        self.url_base = url_base

    def request(self, method, url, **kwargs):
        modified_url = self.url_base + url
        return super(Session, self).request(method, modified_url, **kwargs)

s = Session()

class BaseTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cwd = os.path.dirname(os.path.realpath(__file__))
        # backup original config.lua
        shutil.move(os.path.join(cwd, '../src/config.lua'), os.path.join(cwd, '../src/config.lua.bak'))
        # copy config.py for test
        shutil.copyfile(os.path.join(cwd, 'config.lua'), os.path.join(cwd, '../src/config.lua'))

    @classmethod
    def tearDownClass(cls):
        cwd = os.path.dirname(os.path.realpath(__file__))
        shutil.move(os.path.join(cwd, '../src/config.lua.bak'), os.path.join(cwd, '../src/config.lua'))

    def setUp(self):
        # reload nginx and clear mysql before every test case
        clear_db()
        reload_ngx()
    
    # short cut for /post api when post messages to only one queue
    def post(self, messages, queue='queue1', sender='sender1'):
        data = {
            'sender': sender,
            'messages': {
              queue: messages
            }
        }
        s = Session()
        r = s.post('/post', json=data)
        self.assertEqual(r.status_code, 200)
        return r.json()[queue]

    # shortcut for /pull api in the simplest case: listen to one queue without saving result
    def pull(self, queue='queue1', receiver='receiver1', start=1, max_count=1):
        data = {
            "receiver": receiver,
            "queues": {
              queue: {
                "start": start,
                "max": max_count
              }
            }
        }
        s = Session()
        r = s.post('/pull', json=data)
        self.assertEqual(r.status_code, 200)
        messages = r.json()['messages'][queue]
        self.assertLessEqual(len(messages), max_count)
        return messages

    # performe check in a database, inspired by <http://codeception.com/docs/modules/Db>
    def _select(self, table, criteria):
        where = []
        for column, value in criteria.items():
            if value is None:
                where.append("%s is NULL"%column)
            elif type(value)==str:
                where.append("%s='%s'"%(column, value))
            else:
                where.append("%s=%s"%(column, value))
        c=db.cursor()
        sql = 'select * from %s where %s limit 1'%(table, ' and '.join(where))
        # print(sql)
        r = c.execute(sql)
        # r=1 or 0
        return c.fetchone()

    def assertInDatabase(self, table, criteria):
        self.assertIsNotNone(self._select(table, criteria))

    def assertNotInDatabase(self, table, criteria):
        self.assertIsNone(self._select(table, criteria))

