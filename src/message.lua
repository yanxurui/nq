local lrucache = require "resty.lrucache"
local inspect = require "inspect"
local database = require "database"

local log = ngx.log
local ERR = ngx.ERR
local WARN = ngx.WARN
local INFO = ngx.INFO

local _M = {}

local ttl = 60
local hit = 0
local miss = 0

local cache_last_id, err = lrucache.new(10000)
if not cache_last_id then
    return error("failed to create the cache: " .. (err or "unknown"))
end

local cache_processing_num, err = lrucache.new(10000)
if not cache_processing_num then
    return error("failed to create the cache: " .. (err or "unknown"))
end
local cache_message, err = lrucache.new(100000)
if not cache_message then
    return error("failed to create the cache: " .. (err or "unknown"))
end


function _M.get_last_id(queue, receiver)
    local key = receiver and queue..'.'..receiver or queue
    log(INFO, key)
    local last_id = cache_last_id:get(key)
    if last_id then
        hit = hit + 1
        return last_id
    end

    log(INFO, 'query last id of ', key)
    local sql
    if receiver then
        sql = string.format("select max(m_id) from %s_rst where receiver=\'%s\'", queue, receiver)
    else
        sql = string.format('select max(id) from %s_msg', queue)
    end

    local db = database.connect()
    if not db then
        return nil, 'failed to connect to mysql'
    end
    local res, err, errcode, sqlstate = db:query(sql)
    log(INFO, 'query result: ', inspect(res))
     
    local max
    if res then
        max = res[1][receiver and 'max(m_id)' or 'max(id)']
        if max == ngx.null then
            -- there is no record in the table
            max = 0
        end
    else
        if errcode == 1146 then
            -- Table doesn't exist
            max = 0
        else
            log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
            return nil, 'mysql error' -- can not be replaced by only `return`
        end
    end
    cache_last_id:set(key, max, ttl)
    miss = miss + 1
    return max
end

local function set_last_id(queue, receiver, last_id, count)
    local key = receiver and queue..'.'..receiver or queue
    log(INFO, key, ':', last_id, ',', count)
    local old_last_id = cache_last_id:get(key)
    if receiver then
        assert(count==nil)
        if not old_last_id then
            return
        end
        if last_id <= old_last_id then
            -- this can happen when retrieve the same message again
            return
        end
    else
        -- compute according to insert_id and affected_rows
        last_id = last_id + count - 1
        if old_last_id and last_id<=old_last_id then
            return
        end
    end

    cache_last_id:set(key, last_id, ttl)
end


local function get_processing_num(queue, receiver)
    local key = queue..'.'..receiver
    log(INFO, key)
    local num = cache_processing_num:get(key)
    if num then
        hit = hit + 1
        return num
    end
    log(INFO, 'query processing number of ', key)
    local db = database.connect()
    if not db then
        return nil, 'failed to connect to mysql'
    end
    local sql = string.format([[select count(*) from %s_rst
where receiver='%s' and status='processing']], queue, receiver)
    local res, err, errcode, sqlstate = db:query(sql)
    print(inspect(res))
    local num
    if res then
        num = res[1]['count(*)']
        if num == ngx.null then
            num = 0
        else
            num = tonumber(num)
        end
    else
        if errcode == 1146 then
            num = 0
        else
            log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
            return nil, 'mysql error'
        end
    end
    cache_processing_num:set(key, num)
    miss = miss + 1
    return num
end


local function update_processing_num(queue, receiver, count)
    local key = queue..receiver
    local old_num = cache_processing_num:get(key)
    if old_num then
        num = old_num + count
        cache_processing_num:set(key, num)
        assert(num>=0)
    end
end

local function set_messages(queue, messages)
    for i, message in ipairs(messages) do
        cache_message:set(queue..':'..message['id'], message)
    end
end


function _M.get_messages(queue, receiver, start, max, retry_num)
    assert(start>=0)
    assert(max>=1)

    local result = {}
    local index = string.format('%s:%d,%d', queue, start, max)
    log(INFO, index)

    local db = database.connect()
    if not db then
        return 500, 'failed to connect to mysql'
    end

    for id = start,start+max-1 do
        local message = cache_message:get(queue..':'..id)
        if message then
            table.insert(result, message)
        else
            -- 1. select from mysql if cache miss
            log(INFO, 'message #', id, ' miss, query message ', index)
            local sql = string.format('select * from %s_msg where id >= %d limit %d', queue, start, max)
            log(INFO, 'sql: ', inspect(sql))
            local err, errcode, sqlstate
            result, err, errcode, sqlstate = db:query(sql)
            assert(next(result))
            print('res: ', inspect(result))
            if not result then
                log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
                return 500, 'mysql error'
            end
            set_messages(queue, result)
            break
        end
    end

    -- 2. insert result into mysql
    local values = {}
    local sql
    if retry_num > 0 then
        for i, message in ipairs(result) do
            table.insert(values, string.format("(%d, \'%s\')", message['id'], receiver))
        end
        sql = string.format([[insert into %s_rst(m_id, receiver)
values%s]], queue, table.concat(values, ','))
    else
        -- handle the special case when retry_num is 0
        for i, message in ipairs(result) do
            table.insert(values, string.format("(%d, \'%s\', \'failed\')", message['id'], receiver))
        end
        sql = string.format([[insert into %s_rst(m_id, receiver, status)
values%s]], queue, table.concat(values, ','))
    end
    log(INFO, 'sql: ', inspect(sql))

    local res, err, errcode, sqlstate = db:query(sql)
    print('res: ', inspect(res))
    if not res then
        log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end

    -- 3. update queue.recv.last and queue.recv.processing
    set_last_id(queue, receiver, result[#result]['id'])
    if retry_num > 0 then
        update_processing_num(queue, receiver, #result)
    end

    database.keepalive(db)

    return 200, result
end

function _M.post_messages(queue, sender, messages)
    local ids = {}

    -- 1. bulk insert into mysql
    local db = database.connect()
    if not db then
        return 500, 'failed to connect to mysql'
    end

    local values = {}
    for i, message in ipairs(messages) do
        -- todo: sender is nil
        table.insert(values, string.format([[('%s', '%s')]], sender, message))
    end
    local sql = string.format([[insert into %s_msg(sender, message) values%s]],
        queue, table.concat(values, ','))
    local flag = 0
    ::insert::
    flag = flag + 1
    if flag > 3 then
        return 500, 'dead loop'
    end
    local res, err, errcode, sqlstate = db:query(sql)
    print('res: ', inspect(res))
    if not res then
        -- error code 1146 means "Message: Table '%s.%s' doesn't exist"
        if errcode == 1146 then
            -- create table on fly
            log(ngx.NOTICE, 'create table for queue: ', queue)
            local template = [[create table %s_msg(
id INT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
sender VARCHAR(20) NOT NULL,
tag SET('a', 'b', 'c', 'd'),
created_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
message MEDIUMBLOB NOT NULL
);
create table %s_rst(
m_id INT UNSIGNED,
receiver VARCHAR(20) NOT NULL,
created_time DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_time DATETIME(3) NOT NULL ON UPDATE CURRENT_TIMESTAMP,
status ENUM('processing', 'finished', 'failed') NOT NULL DEFAULT 'processing',
fail_count TINYINT UNSIGNED DEFAULT 0,
result VARBINARY(1024),
PRIMARY key(m_id, receiver),
FOREIGN KEY(m_id) REFERENCES %s_msg(id),
INDEX receiver_idx(receiver),
INDEX status_idx(status)
)
]]
            local create_sql = string.format(template, queue, queue, queue)
            local res, err, errcode, sqlstate = db:query(create_sql)
            print('res: ', inspect(res))
            if not res then
                if errcode == 1050 then
                    log(INFO, err)
                    -- Table '%s.%s' already exists
                    goto insert -- this could lead to dead loop
                end
                log(ERR, "failed to create message table: ", err, ": ", errcode, ": ", sqlstate, ".")
                return 500, 'mysql error'
            end
            assert(err=='again')
            res, err, errcode, sqlstate = db:read_result(create_sql)
            print('res: ', inspect(res))
            if not res then
                log(ERR, "failed to create result table: ", err, ": ", errcode, ": ", sqlstate, ".")
                return 500, 'mysql error'
            end
            goto insert
        end
        log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end
    -- get the array of IDs in bulk insert
    -- see: https://stackoverflow.com/questions/7333524/how-can-i-insert-many-rows-into-a-mysql-table-and-return-the-new-ids
    local count = #messages
    assert(res.affected_rows==count)
    local messages_cache = {}
    local id
    for i = 0, count-1 do
        id = res.insert_id + i
        table.insert(ids, id)
        table.insert(messages_cache,
            {
                id=id, -- a little confused
                sender=sender,
                message=messages[i+1]
            }
        )
    end

    -- 2. update queue.last
    set_last_id(queue, nil, res.insert_id, count)

    -- 3. set LRU cache
    -- todo: created_time miss
    set_messages(queue, messages_cache)

    database.keepalive(db)
    return 200, ids
end

function _M.save_results(queue, receiver, results)
    local db = database.connect()
    if not db then
        return 500, 'failed to connect to mysql'
    end

    local sqls = {}
    for id, result in pairs(results) do
        table.insert(sqls, string.format([[update %s_rst
set status='finished', result='%s'
where m_id=%s and receiver='%s']], queue, result, id, receiver))
    end
    local sql = table.concat(sqls, ';\n')
    print('sql: ', sql)
    local res, err, errcode, sqlstate = db:query(sql)
    log(INFO, 'res: ', inspect(res))
    if not res then
        log(ERR, "bad result #1: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end
    local i = 1
    while err == "again" do
        i = i + 1
        res, err, errcode, sqlstate = db:read_result()
        log(INFO, 'res: ', inspect(res))
        if not res then
            ngx.log(ngx.ERR, "bad result #", i, ": ", err, ": ", errcode, ": ", sqlstate, ".")
            return 500, 'mysql error'
        end
    end
    assert(i==#sqls)
    update_processing_num(queue, receiver, -i)
    return 200
end

function _M.get_timeout_message(queue, receiver, fail_timeout, retry_num, max)
    local processing, err = get_processing_num(queue, receiver)
    if not processing then
        return 500, err
    elseif processing==0 then
        return 404
    end

    local db = database.connect()
    if not db then
        return 500, 'failed to connect to mysql'
    end

    local sql = string.format([[select %s_msg.*, %s_rst.fail_count from %s_rst, %s_msg
where receiver='%s' and status='processing' and
(
(fail_count=0 and %s_rst.created_time<DATE_SUB(NOW(3), INTERVAL %.3f SECOND_MICROSECOND))
or
(fail_count<>0 and updated_time<DATE_SUB(NOW(3), INTERVAL %.3f SECOND_MICROSECOND))
)
and
%s_msg.id=%s_rst.m_id
limit %d]], queue, queue, queue, queue, receiver, queue, fail_timeout, fail_timeout, queue, queue, max)
    print(sql)
    local res, err, errcode, sqlstate = db:query(sql)
    print('res: ', inspect(res))
    if not res then
        log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end

    if next(res) == nil then
        return 404
    end

    local result = {}

    -- update retry messages
    local sqls = {}
    local fail_count
    local failed_num=0
    for i, r in ipairs(res) do
        fail_count = r['fail_count']+1
        if fail_count >= retry_num then
            -- turn to failed at last retry
            table.insert(sqls, string.format([[update %s_rst
set fail_count=%d, status='failed'
where m_id=%s and receiver='%s']], queue, fail_count, r['id'], receiver))
            failed_num = failed_num + 1
        else
            table.insert(sqls, string.format([[update %s_rst
set fail_count=%d
where m_id=%s and receiver='%s']], queue, fail_count, r['id'], receiver))
        end
        if fail_count <= retry_num then
            r['fail_count'] = nil
            table.insert(result, r)
        else
            -- fail_count > retry_num happen when retry_num shrink
            log(WARN, queue, r['id'])
        end
    end
    local sql = table.concat(sqls, ';\n')
    print('sql: ', sql)
    local res, err, errcode, sqlstate = db:query(sql)
    log(INFO, 'res: ', inspect(res))
    if not res then
        log(ERR, "bad result #1: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end
    local i = 1
    while err == "again" do
        i = i + 1
        res, err, errcode, sqlstate = db:read_result()
        log(INFO, 'res: ', inspect(res))
        if not res then
            ngx.log(ngx.ERR, "bad result #", i, ": ", err, ": ", errcode, ": ", sqlstate, ".")
            return 500, 'mysql error'
        end
    end
    assert(i==#sqls)
    if failed_num > 0 then
        update_processing_num(queue, receiver, -failed_num)
    end
    if next(result) == nil then
        return 404
    end
    return 200, result
end


return _M
