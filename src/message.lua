local lrucache = require "resty.lrucache"
local inspect = require "inspect"
local database = require "database"

local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO

local _M = {}

local ttl = 60
local hit = 0
local miss = 0

local cache_last_id, err = lrucache.new(10000)
if not cache_last_id then
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
            return nil, 'mysql error' -- can not replaced by just `return`
        end
    end
    cache_last_id:set(key, max, ttl)
    miss = miss + 1
    return max
end

local function set_last_id(queue, receiver, last_id, count)
    local key = receiver and queue..'.'..receiver or queue
    log(INFO, key, ':', last_id, ',', count)
    if not count then
        -- set default value
        count = 1
    end
    local old_last_id = cache_last_id:get(key)
    if receiver then
        if not old_last_id then
            return
        end
        if last_id <= old_last_id then
            -- this can happen when retrieve the same message again
            return
        end
    else
        -- check consistency for queue's last id
        if old_last_id then
            assert(last_id==old_last_id+1)
        end
    end
    -- compute according to insert_id and affected_rows
    last_id = last_id + count - 1
    cache_last_id:set(key, last_id, ttl)
end

local function set_messages(queue, messages)
    for i, message in ipairs(messages) do
        cache_message:set(queue..':'..message['id'], message)
    end
end

function _M.get_messages(queue, receiver, start, max)
    assert(start>=0)
    assert(max>=1)

    local result = {}
    local index = string.format('%s:%d,%d', queue, start, max)
    log(INFO, index)

    local db = database.connect()
    if not db then
        return 500, 'failed to connect to mysql'
    end

    -- 1. select from mysql if cache miss
    for id = start,start+max-1 do
        local message = cache_message:get(queue..':'..id)
        if message then
            table.insert(result, message)
        else
            log(INFO, 'message #', id, ' miss, query message ', index)
            local sql = string.format('select * from %s_msg where id >= %d limit %d', queue, start, max)
            log(INFO, 'sql: ', inspect(sql))
            result, err, errcode, sqlstate = db:query(sql)
            log(INFO, 'res: ', inspect(result))
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
    for i, message in ipairs(result) do
        table.insert(values, string.format("(%d, \'%s\')", message['id'], receiver))
    end

    sql = string.format([[insert into %s_rst(m_id, receiver)
values%s]], queue, table.concat(values, ','))
    log(INFO, 'sql: ', inspect(sql))
    local res, err, errcode, sqlstate = db:query(sql)
    log(INFO, 'res: ', inspect(res))
    if not res then
        log(ERR, "bad result: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end

    -- 3. update queue.recv.last
    set_last_id(queue, receiver, result[#result]['id'])

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
        table.insert(values, string.format("(\'%s\', \'%s\')", sender, message))
    end
    local sql = string.format("insert into %s_msg(sender, message) values%s", queue, table.concat(values, ','))
    ::insert::
    local res, err, errcode, sqlstate = db:query(sql)
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
status ENUM('pending', 'finished') NOT NULL DEFAULT 'pending',
retry_num TINYINT UNSIGNED DEFAULT 0,
result VARBINARY(1024),
FOREIGN KEY(m_id) REFERENCES %s_msg(id),
primary key(m_id, receiver)
);
]]
            local create_sql = string.format(template, queue, queue, queue)
            local res, err, errcode, sqlstate = db:query(create_sql)
            log(INFO, '++++', inspect(res))
            if not res then
                log(ERR, "failed to create message table: ", err, ": ", errcode, ": ", sqlstate, ".")
                return 500, 'mysql error'
            end
            assert(err=='again')
            res, err, errcode, sqlstate = db:read_result(create_sql)
            print('+++', inspect(res))
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
    log(INFO, 'sql: ', sql)
    local res, err, errcode, sqlstate = db:query(sql)
    log(INFO, 'res: ', inspect(res))
    if not res then
        log(ERR, "bad result #1: ", err, ": ", errcode, ": ", sqlstate, ".")
        return 500, 'mysql error'
    end
    local i = 2
    while err == "again" do
        res, err, errcode, sqlstate = db:read_result()
        log(INFO, 'res: ', inspect(res))
        if not res then
            ngx.log(ngx.ERR, "bad result #", i, ": ", err, ": ", errcode, ": ", sqlstate, ".")
            return 500, 'mysql error'
        end
        i = i + 1
    end
    return 200
end

return _M
