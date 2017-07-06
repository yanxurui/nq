-- listen to queues until there are new messages

local semaphore = require "ngx.semaphore"

local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO

local _M = {
}

local sems = {}

function _M.sub(queues, timeout)
    local sem = semaphore.new()
    for i, queue in ipairs(queues) do
        local q_sems = sems[queue]
        if q_sems == nil then
            q_sems = {}
            -- use weak table to enable garbage collection of this semaphore as soon as possible
            setmetatable(q_sems, {__mode='v'})
            sems[queue] = q_sems
        end
        table.insert(q_sems, sem)
    end
    local ok, err = sem:wait(timeout)
    if not ok and err ~= 'timeout' then
        log(ERR, "failed to wait on sema: ", err)
    end
    return ok, err
end

function _M.pub(queue)
    local q_sems = sems[queue]

    if q_sems == nil then
        log(INFO, 'no one is now listening to ', queue)
        return
    end
    local c = 0
    for i, sem in ipairs(q_sems) do
        local count = sem:count()
        log(INFO, 'find ', -count, ' waiters at #', i , ' on queue: ', queue)
        if count < 0 then
            -- someone is waiting there
            sem:post(1)
            c = c + 1
        end
    end
    log(INFO, 'wake up ', c, ' waiters')
    sems[queue] = nil
end

return _M
