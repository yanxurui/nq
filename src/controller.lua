local inspect = require "inspect"

local message = require "message"
local lock = require "lock"
local pubsub = require "pubsub"
local config = require "config"

local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO

local _M = {}

function _M.post(sender, messages)
    log(INFO, 'put messages into mysql')
    local result = {}
    for queue, message_list in pairs(messages) do
        if type(queue) ~= 'string' or type(message_list) ~= 'table' then
            return 400, 'bad paramter `messages`'
        end
        if #message_list > 0 then
            local status, res = message.post_messages(queue, sender, message_list)
            if status == 200 then
                log(INFO, 'wake up all waiters for ', queue)
                pubsub.pub(queue)
                result[queue]=res
            else
                result['_error'] = res
                return status, result
            end
        end
    end
    if next(result) == nil then
        return 400, 'messages should not be empty'
    end
    return 200, result
end

local function has_next(queue, receiver, start)
    local receiver_last_id, err = message.get_last_id(queue, receiver)
    if not receiver_last_id then
        return nil, nil, err
    end
    local queue_last_id, err = message.get_last_id(queue)
    if not queue_last_id then
        return nil, nil, err
    end
    assert(receiver_last_id<=queue_last_id)

    if start == -1 then
        -- only receive the last message
        start = queue_last_id
    end
    assert(start>=1)
    local next_id = math.max(start, receiver_last_id+1)

    if next_id <= queue_last_id then
        return next_id, queue_last_id-next_id+1
    end
end

function _M.get(receiver, queues, timeout)
    local messages = {}
    local queue_names = {}
    local n = 0

    for queue, params in pairs(queues) do
        n = n + 1
        queue_names[n] = queue
        local start = params['start'] or 1
        if start == 0 then
            log(INFO, 'ignore existing messages')
            local queue_last_id, err = message.get_last_id(queue)
            if not queue_last_id then
                return 500, err
            end
            params['start'] = queue_last_id + 1
            goto continue
        end
        local max = params['max'] or 1
        log(INFO, 'check ', queue, '.', receiver, ' for available messages')
        local next_id, available, err = has_next(queue, receiver, start)
        if next_id then
            local lock_key = queue..'.'..receiver
            local ok, released = lock.lock(lock_key)
            if released then
                log(INFO, 'some one else has just acquired and released the lock, check again')
                next_id, available, err = has_next(queue, receiver, start)
                if err then
                    lock.unlock(lock_key)
                    return 500, err
                elseif not next_id then
                    log(INFO, 'no message left, continue')
                    lock.unlock(lock_key)
                    goto continue
                end
            end
            local status, res = message.get_messages(queue, receiver, next_id, math.min(max, available))
            if status ~= 200 then
                lock.unlock(lock_key)
                return status, res
            end
            messages[queue] = res
            lock.unlock(lock_key)
        elseif err then
            return 500, err
        end
        ::continue::
    end
    if next(messages) == nil then
        log(INFO, 'no queue has new message, check timeout')
        -- todo

        log(INFO, 'no queue has new message, wait')
        timeout = timeout or config.beat_interval
        local ok ,err = pubsub.sub(queue_names, timeout)
        if ok then
            log(INFO, 'I am waked up because some queue is updated')
            return _M.get(receiver, queues)
        elseif err == 'timeout' then
            log(INFO, 'waiting new messages timeout')
            local i, queue
            local result={}
            for i = 1, #queue_names do
                queue = queue_names[i]
                result[queue] = message.get_last_id(queue)
            end
            return 404, result
        else
            return 500, err
        end
    end
    return 200, {messages=messages}
end

function _M.save_results(receiver, results)
    for queue, q_results in pairs(results) do
        local status ,res = message.save_results(queue, receiver, q_results)
        if status ~= 200 then
            return status, res
        end
    end
    return 200
end


return _M
