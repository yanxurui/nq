-- parse request, check params

local cjson_safe = require "cjson.safe"
local inspect = require 'inspect'

local controller = require 'controller'

local log = ngx.log
local ERR = ngx.ERR
local INFO = ngx.INFO

local method = ngx.req.get_method()
local request_uri = ngx.var.request_uri
local data, err = cjson_safe.decode(ngx.req.get_body_data())

local status, res

if data == nil then
    log(ngx.ERR, 'failed to decode json: ', err)
    status = ngx.HTTP_BAD_REQUEST
    res = err
    goto done
end

if method == 'POST' then
    if request_uri == '/post' then
        log(INFO, 'POST data: ', inspect(data))
        local sender = data['sender']
        local messages = data['messages']
        if type(messages) ~= 'table' then
            status = ngx.HTTP_BAD_REQUEST
            res = 'bad parameter `messages`'
            goto done
        end
        status, res = controller.post(sender, messages)
    elseif request_uri == '/pull' then
        log(INFO, 'PULL data: ', inspect(data))
        local receiver = data['receiver']
        if not receiver then
            status = ngx.HTTP_BAD_REQUEST
            res = 'receiver name is required'
            goto done
        end
        
        local results = data['results']        
        if results then
            log(INFO, 'save results for receiver: ', receiver)
            status, res = controller.save_results(receiver, results)
            if status ~= 200 then
                goto done
            end
        end

        local queues = data['queues']
        local timeout = data['timeout']
        if queues then
            log(INFO, 'pull messages for receiver: ', receiver)
            status, res = controller.get(receiver, queues, timeout)
        end

        if not status then
            status = HTTP_BAD_REQUEST
            res = 'queues or results are required'
        end
    else
        ngx.exit(ngx.HTTP_BAD_REQUEST)
    end
else
    ngx.exit(ngx.HTTP_METHOD_NOT_IMPLEMENTED)
end


::done::
ngx.status = status
if res then
    ngx.print(cjson_safe.encode(res))
end



