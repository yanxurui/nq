import time

from mq import queue

s = time.time()

for i in range(1000000):
	q = queue('mymq')
	q.put('0'*100)

print(time.time() - s)

## 1024B * 100000
# 1.14815497398
# 1.08567690849

## 100B * 1000000
# 4.49793386459
# 2.78889203072