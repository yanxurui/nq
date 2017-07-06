## Introduction
NQ(nginx as a queue) is not solely a message brocker.

## Advantages
* simple but powerful, easy to use&monitor
* high performance&reliability
* message and result are both persistent
* support common patterns: RPC, pub/sub, delayed or distributed jobs
* http api, easy to debug
* well tested

## Restful API

### POST
```
POST /post
{
  "sender": "sender1",
  "messages": {
    "queue1": ["blablabla", "hello world"],
    ...
  }
}
```
sender: unique name of sender
messages:
A dict with queue name as key and a list of messages as value. Message must be string.

```
200
{
  "queue1":[2, 3],
  ...
}
```
Return a list of ids which increments from 1 for every queue.

If an error occurs during saving messages, NQ returns immediatelly
with the correct status code set and an extra field `_error` indicating the error message. For example:

```
500
{
  "queue1":[1,2],
  "_error":"mysql error"
}
```
In most cases, `_error` is the only field in the response.


### PULL
```
POST /pull
Req:
{
  "receiver": "receiver1",
  "timeout": 10,
  "queues": {
    "queue1": {
      "start": 3,
      "max": 10
    },
    ...
  },
  "results": {
    "queue1": {
      "1": "got it",
      ...
    }
  }
}
```
pull new messages
save results
or do both at the same time.

receiver: unique name of receiver. If more than one receiver have the same name, only one of them will receive the same message. This mechanism is relied by multiple workers to do distributed jobs.

timeout(optional, default:60): A time in seconds(float) to wait for new message. Status 404 will be returned if timeout expires. A value of 0 means to return immediately without blocking.

queues:
queues is a dict with queue name as key and a dict of optional params as value.

start(optional, defailt:0): A message id from where to retrieve the message.

NQ makes sure all messages are delivered in the order they are enqueued and the same receiver will never get messages repeatedly.

There are 3 special values:
* 0: The default value is 0, which indicates restore from the last received message. It will retrieve messages from the beginning of the queue if this receiver has not pulled any message before.
* -1: return the last unconsumed message. If there is no available message, wait next message.
* -2: ignore all messages that already exist and wait for new messages instead.

max(optional, default:1): The max number of messages in this queue to return. The actual value depends on the number of available messages.

results:
`results` is a dict with queue name as key and a dict of results as value. Every result item is a pair of message id and result string.

The message id here is a string since JSON only allows key names to be strings.


```
200
{
  "messages": {
    "queue1": [
      {
         "id": 3,
         "sender": "sender1",
         "tag": 4,
         "message": "blablabla",
         "created_time": 2017-06-22
      },
      {...}
    ],
    ...
  }
  
}
```
`created_time` may be ommitted if the message is retrived from cache without querying myql.

If no message is available when timeout expires, the response should be:
```
404
{
  "queue1": 8,// suppose 8 is id of the last message in queue1
  ...
}
```
some specical values of id can be
* 0: there is no message yet
* NULL: an error occurs when query the last id of this queue

