-- parse request, check params

local cjson_safe = require "cjson.safe"
-- local inspect = require 'inspect'

local sender_cls = require 'sender'
local receiver_cls = require 'receiver'

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
        -- print('POST data: ', inspect(data))
        local sender = data['sender']
        local messages = data['messages']
        if type(messages) ~= 'table' then
            status = ngx.HTTP_BAD_REQUEST
            res = 'bad parameter `messages`'
            goto done
        end
        local s = sender_cls.new(sender)
        status, res = s:post(messages)
    elseif request_uri == '/pull' then
        -- print('PULL data: ', inspect(data))
        local receiver = data['receiver']
        if not receiver then
            status = ngx.HTTP_BAD_REQUEST
            res = 'receiver name is required'
            goto done
        end

        local r = receiver_cls.new(receiver, data['timeout'])
        
        local results = data['results']        
        if results and next(results) then
            log(INFO, 'save results for receiver: ', receiver)
            status, res = r:save_results(results)
            if status ~= 200 then
                goto done
            end
        end

        local queues = data['queues']
        if queues and next(queues) then
            log(INFO, 'pull messages for receiver: ', receiver)
            status, res = r:get(queues)
        end

        if not status then
            status = HTTP_BAD_REQUEST
            res = 'queues or results are required'
        end
    elseif request_uri == '/query' then
        -- todo
        status = nx.HTTP_NOT_FOUND
    else
        ngx.exit(ngx.HTTP_BAD_REQUEST)
    end
else
    ngx.exit(ngx.HTTP_METHOD_NOT_IMPLEMENTED)
end


::done::
ngx.status = status
if res then
    if status == 500 and type(res) == 'string' then
        res = {_error=res}
    end
    ngx.print(cjson_safe.encode(res))
end



