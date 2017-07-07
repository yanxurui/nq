-- listen to queues until there are new messages

local semaphore = require "ngx.semaphore"

local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO

local _M = {
}

local sems = {}

function _M.sub(sem, queue)
    local q_sems = sems[queue]
    if q_sems == nil then
        q_sems = {}
        -- use weak table to enable garbage collection of this semaphore as soon as possible
        setmetatable(q_sems, {__mode='v'})
        sems[queue] = q_sems
    end
    table.insert(q_sems, sem)
end

function _M.pub(queue)
    local q_sems = sems[queue]

    if q_sems == nil then
        log(INFO, 'no one is now listening to ', queue)
        return
    end
    for i, sem in ipairs(q_sems) do
        local count = sem:count()
        print('find ', count, ' at #', i , ' on queue: ', queue)
        sem:post(1)
    end
    sems[queue] = nil
end

return _M
