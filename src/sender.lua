-- class sender
local message = require "message"
local pubsub = require "pubsub"

local log = ngx.log
local INFO = ngx.INFO

local _M = {}

local mt = {__index = _M}

function _M.new(name)
	local o = {}
	o.name = name
	setmetatable(o, mt)
	return o
end

function _M.post(self, messages)
    log(INFO, 'put messages into mysql')
    local result = {}
    for queue, message_list in pairs(messages) do
        if type(queue) ~= 'string' or type(message_list) ~= 'table' then
            return 400, 'bad paramter `messages`'
        end
        if #message_list > 0 then
            local status, res = message.post_messages(queue, self.name, message_list)
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

return _M
