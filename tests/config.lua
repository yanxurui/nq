local _M = {
    mysql={
        host='127.0.0.1',
        port=3306,
        user='root',
        password='',
        database='nq_test'
    },
    receiver={
        timeout=0.5,
        retry_num=2,
        fail_timeout=0.5
    }
}

return _M
