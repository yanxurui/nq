## Introduction
NQ(nginx as a queue) is a message queue based on ngx-lua and MYSQL. It provides REST API. Messages are pulled by HTTP long polling.
As most message brokers do, NQ can work as a FIFO queue or PUB/SUB pattern.
NQ is not only a message broker, it persists messages in mysql as well as receiver's processing time and result.


## Advantages
* simple but powerful, easy to use&monitor
* unacknowledged messages will be retried until failed
* support common patterns: pub/sub, producers and consumers, delayed or distributed jobs
* high reliability, no message lost. message and result are both persisted
* http REST api: client can use most existing tools. also easy to debug
* well tested


## Retry
A `processing` job will be retried after `fail_timeout` seconds unless it an acknowledgement is received or exceeds `retry_num`. Retry happen in a lazy mode instead of as soon as possible. A receiver can only get retry tasks when there is no new messages available for this receiver.

A job is considered failed if it is retried `retry_num` times which is reflected by the value of `fail_count` field in result table.
If the param `retry_num` is set to 0, the status of a message will become failed the moment it is retrieved until it is acknowledged later. This may be kind of confused at first.


## Install
### step 1: download
clone this repository and all its submodules to where you want to install. Please make sure the user(nobody) who run nginx worker has read and execute permission for this directory and **all of its parent directories**.
```
git clone --recursive git@github.com:yanxurui/nq.git
cd nq
```

### step 2: install nginx & lua libs
execute INSTALL script (you should adjust it according to your needs)
```
./INSTALL.sh
```
it does the following 3 things:

1. install luajit2.1 to `/usr/local`
2. compile nginx with the latest lua module(to be exact, v0.10.9 or later which requires luajit 2.1) and install it to `/opt/nginx`
3. install dependencies(including compiling lua-cjson)

### step 3: start nginx
assume `/opt/nginx/sbin/nginx` is the nginx you install in step 1 and `/opt/nq` is where you clone this repo to in step 2.
```
/opt/nginx/sbin/nginx -p /opt/nq/
```


## Config
`conf/nginx.conf` is configuration for nginx specialized for this program and you are not supposed to modify it except listen port(default is 8001). **There can be only one worker process since nq uses semaphore to synchronize senders and receivers and furthermore nq uses a lot of cache in lua level for the sake of performance**.

`src/config.lua` is the global configuration you need to modify.
The meaning of mysql configuration is obvious. First you need to create a database in mysql and then grant privilege to a user.
```sql
create database nq character set utf8;
grant all privileges on nq.* to 'nq'@'localhost' identified by '123456';
FLUSH PRIVILEGES;
```
receiver configuration is described in [REST API](#PULL). This is the global configuration for all receivers.


## Test
Tests are written using python unittest.

### install dependencies
MySQL-python requires mysql-devel, on CentOS:
```
yum install mysql-devel
```

install python packages(you'd better do this in a python virtual envrionment created by `virtualenv`)
```
pip install -r requirements.txt
```

### config
Create a database nq_test for the purpose of test. Change `tests/config.lua` and `tests/config.py` to fit your case.

### run test
```
python -m unittest discover -v
```


## REST API
It's recommended to use a client specific to a language instead of these apis directly. Clients are well written to be efficient and deal with various exceptions. Only python and php clients are available so far.

This documentation refers message as job or task depending on application scenario. Receiver is the same as worker, subscriber or consumer meanwhile sender is the same as publisher or producer.

### POST
post messages to one or more queues. Messages are saved in mysql.

#### Req:
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
**sender**(optional, default:NULL): identity of sender, it's recommended to use this to distinguish between different senders if there are more than one senders enqueuing the same queue.

**messages**: A dict with **queue name** as key and **a list of messages** as value. Message must be string.

#### Resp:
```
200
{
  "queue1":[2, 3],
  ...
}
```
Return a list of ids which increments from 1 for every queue.

If an error occurs during saving messages, NQ returns immediatelly with the correct status code set and an extra field `_error` indicating the error message. For example:

```
500
{
  "queue1":[1, 2],
  "_error":"mysql error"
}
```
In most cases, `_error` is the only field in the response.


### PULL
pull new messages, save results or do both at the same time. Acknowledgement is made by the way of sending results.
Receivers are blocked if there are no unreceived messages yet unless timeout is set to 0.

#### Req:
```
POST /pull
{
  "receiver": "receiver1",
  "timeout": 10,
  "queues": {
    "queue1": {
      "start": 3,
      "max": 10,
      "retry_num": 3,
      "fail_timeout": 60
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

**receiver**: identity of a receiver. Different receivers receive messages independently. If more than one receiver have the same name, only one of them will receive the same message. This mechanism is relied by multiple workers to do distributed jobs.

**timeout**(optional, default:60): A time in seconds(float) to wait for new message. Status `204` will be returned if timeout expires. A value of 0 means to return immediately without blocking.

**queues**: a dict with queue name as key and a dict of optional params as value.
Below are all parameters and their meaning.

  * **start**(optional, defailt:0): A message id(starts from 1) from where to retrieve the message. **NQ makes sure all messages are delivered in the order they are enqueued and the same receiver will never get messages repeatedly**. Thus if a message has already been delivered to the same worker, all messages before it including itself will not be delivered again though start is less than the id of that message.

    There are 3 special values:
    * 0: The default value is 0, which indicates restore from the last received message. It will retrieve messages from the beginning of the queue if this receiver has not pulled any message before.
    * -1: return the last unconsumed message. If there is no available message, wait next message.
    * -2: ignore all messages that already exist and wait for new messages instead.

  * **max**(optional, default:1): The max number of messages in this queue to return. The actual number of messages returned depends on the number of available messages.

  * **retry_num**(optional, default:2): the max number of retry times.
  * **fail_timeout**(optional, default:120): a time of seconds(float) after which a message is considered failed after it is received but not acknowledged. Details of retry_num and fail_timeout are described in [retry section](#Retry).

**results**: a dict with queue name as key and a dict of results as value. Every result item is a pair of message id and result string.

Note
1. the message id here is a string since JSON only allows key names to be strings.

#### Resp:
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
Note
1. `created_time` may be missing if the message is retrived from cache without querying myql.


