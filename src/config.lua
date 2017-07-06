local _M = {
	mysql={
		host='127.0.0.1',
		port=3306,
		user='root',
		password='',
		database='nq'
	},
	beat_interval=10,
	fail_retry_count=3,
}

return _M
