local _M = {
    mysql={
        host='127.0.0.1',
        port=3306,
        user='nq',
        password='123456',
        database='nq'
    },
    receiver={
        timeout=60,
        retry_num=2,
        fail_timeout=120
    }
}

return _M
