local _M = {
    mysql={
        host='127.0.0.1',
        port=3306,
        user='root',
        password='',
        database='nq'
    },
    receiver={
        timeout=60,
        retry_num=3,
        fail_timeout=120
    }
}

return _M
