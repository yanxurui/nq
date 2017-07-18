/* the mysql schemas and sql statements used in this project
   these can be used to debug and optimise index */

-- database
create database nq character set utf8;
grant all privileges on nq.* to 'nq'@'localhost' identified by '123456';
FLUSH PRIVILEGES;

-- tables
create table queue1_msg(
id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
sender VARCHAR(20) NOT NULL,
created_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
message MEDIUMBLOB NOT NULL
) ENGINE = MyISAM;

create table queue1_rst(
m_id INT UNSIGNED,
receiver VARCHAR(20) NOT NULL,
created_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_time DATETIME(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP,
status ENUM('processing', 'finished', 'failed') NOT NULL DEFAULT 'processing',
fail_count TINYINT UNSIGNED DEFAULT 0,
result VARBINARY(1024),
PRIMARY key(m_id, receiver),
FOREIGN KEY(m_id) REFERENCES mq1_msg(id),
INDEX receiver_idx(receiver),
INDEX status_idx(status)
) ENGINE = MyISAM;

-- post messages
insert into queue1_msg(sender, message)
values('sender1', 'foo'), ('sender1', 'bar');

-- get last id
select max(id) from queue1_msg;
select max(m_id) from queue1_rst where receiver='receiver1';
-- this should use index merge: intersect(receiver_idx, status_idx)
select count(*) from queue1_rst
where receiver='receiver1' and status='processing';

-- pull messages
select * from queue1_msg where id >= 1 limit 2;

-- save result
insert into queue1_rst(m_id, receiver)
values(1, 'receiver1'),(2, 'receiver1');

-- update result
update queue1_rst
set status='finished', result='done'
where m_id=2 and receiver='receiver1';

-- retry tasks(2 minutes later)
select queue1_msg.*, queue1_rst.fail_count from queue1_rst, queue1_msg
where receiver='receiver1' and status='processing' and
(
(fail_count=0 and queue1_rst.created_time<DATE_SUB(NOW(), INTERVAL 120 SECOND)) 
or
(fail_count<>0 and updated_time<DATE_SUB(NOW(), INTERVAL 120 SECOND))
)
and
queue1_msg.id=queue1_rst.m_id;
-- update fail count(retry num)
update queue1_rst
set fail_count=1
where m_id=1 and receiver='receiver1';
-- also update status of failed tasks
update queue1_rst
set fail_count=1, status='failed'
where m_id=1 and receiver='receiver1';

