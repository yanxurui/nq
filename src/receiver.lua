-- class receiver
local semaphore = require "ngx.semaphore"
local message = require "message"
local pubsub = require "pubsub"
local config = require 'config'
local lock = require "lock"

local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO

local _M = {}

local mt = {__index = _M}

function _M.new(name, timeout)
    local o = {}
    o.name = name
    o.timeout = timeout or config.receiver.timeout
    setmetatable(o, mt)
    return o
end

local function listen(self, queue)
    log(INFO, self.name, ' listen to ', queue)
    if not self.sem then
        self.sem = semaphore.new()
    end
    pubsub.sub(self.sem, queue)
end

local function has_next(self, queue, start)
    local receiver_last_id, err = message.get_last_id(queue, self.name)
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
    -- no message
    -- nil, nil, nil
end

function _M.get(self, queues)
    local messages = {}

    for queue, params in pairs(queues) do
        local lock_key = queue..'.'..self.name
        local start = params['start'] or 1
        local max = params['max'] or 1
        if start == -2 then
            log(INFO, 'ignore existing messages')
            local queue_last_id, err = message.get_last_id(queue)
            if not queue_last_id then
                return 500, err
            end
            params['start'] = queue_last_id + 1
            listen(self, queue)
            goto wait
        end
        log(INFO, 'check ', queue, '.', self.name, ' for available messages')
        local next_id, available, err = has_next(self, queue, start)
        if next_id then
            local ok, res = lock.lock(lock_key)
            if not ok then
                return 500, res
            end
            if res then
                log(INFO, 'some one else has just acquired and released the lock, check again')
                next_id, available, err = has_next(self, queue, start)
                if not next_id then
                    lock.unlock(lock_key)
                    if err then
                        return 500, err
                    end
                    log(INFO, 'no message left, continue')
                    listen(self, queue)
                    goto continue
                end
            end
            local status, res = message.get_messages(
                queue,
                self.name,
                next_id,
                math.min(max, available),
                params['retry_num'] or config.receiver.retry_num
            )
            lock.unlock(lock_key)
            if status ~= 200 then
                return status, res
            end
            messages[queue] = res
        elseif err then
            return 500, err
        else
            listen(self, queue)
        end
        ::continue::
    end

    if next(messages) ~= nil then
        return 200, {messages=messages}
    end

    log(INFO, 'no queue has new message')
    for queue, params in pairs(queues) do
        local lock_key = queue..'.'..self.name..'(timeout)'
        local ok, res = lock.lock(lock_key)
        if not ok then
            return 500, res
        end
        log(INFO, 'retry timeout tasks in ', queue)
        local status, res = message.get_timeout_message(
            queue,
            self.name,
            params['fail_timeout'] or config.receiver.fail_timeout,
            params['retry_num'] or config.receiver.retry_num,
            params['max'] or 1
        )
        lock.unlock(lock_key)
        if status == 200 then
            messages[queue] = res
        elseif status == 404 then
            log(INFO, 'no timeout message available in ', queue)
        else
            return status, res
        end
    end
    if next(messages) ~= nil then
        return 200, {messages=messages}
    end

    ::wait::

    log(INFO, 'wait for new messages')
    assert(self.sem)
    local ok ,err = self.sem:wait(self.timeout)
    if ok then
        log(INFO, 'I am waked up because some queue is updated')
        return self:get(queues)
    elseif err == 'timeout' then
        log(INFO, 'waiting new messages timeout')
        return 204 -- no content
    else
        log(ERR, "failed to wait on sema: ", err)
        return 500, err
    end
end

function _M.save_results(self, results)
    for queue, q_results in pairs(results) do
        local status ,res = message.save_results(queue, self.name, q_results)
        if status ~= 200 then
            return status, res
        end
    end
    return 200
end

return _M
