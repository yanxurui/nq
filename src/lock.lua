-- lock implemented by semaphore

local semaphore = require "ngx.semaphore"

local log = ngx.log
local INFO = ngx.INFO

local _M = {}
local locks = {}
local TIMEOUT = 5

function _M.lock(key, timeout)
	timeout = timeout or TIMEOUT
	log(INFO, key)
	if locks[key] == nil then
		locks[key] = true
		return true
	elseif locks[key] == true then
		-- someone else has obtained the lock
		local sema = semaphore.new()
		locks[key] = sema
		local ok, err = sema:wait(timeout)
		if ok then
			return true, true
		else
			log(ngx.WARN, 'failed to wait on sema: ', err)
			return ok, err
		end
	else
		local sema = locks[key]
		local count = sema:count()
		local ok, err = sema:wait(timeout)
		if ok then
			return true, count<=0
		else
			log(ngx.WARN, 'failed to wait on sema: ', err)
			return ok, err
		end
	end
end

function _M.unlock(key)
	log(INFO, key)
	assert(locks[key] ~= nil)
	if locks[key] == true then
		locks[key] = nil
	else
		locks[key]:post(1)
	end
end

return _M