-- factory function for mysql instance

local mysql = require "resty.mysql"
local config = require "config"

local log = ngx.log
local ERR = ngx.ERR

local _M = {}

function _M.connect()
    local db, err = mysql:new()
    if not db then
        log(ERR, "failed to instantiate mysql: ", err)
        return nil
    end
    db:set_timeout(1000) -- 1 sec
    local ok, err, errcode, sqlstate = db:connect{
        host = config.mysql.host,
        port = config.mysql.port,
        database = config.mysql.database,
        user = config.mysql.user,
        password = config.mysql.password,
        charset = "utf8",
        max_packet_size = 1024 * 1024,
    }
    if not ok then
        log(ERR, "failed to connect: ", err, ": ", errcode, " ", sqlstate)
        return nil
    end
    return db
end

function _M.keepalive(db)
    local ok, err = db:set_keepalive(10000, 100)
    if not ok then
        ngx.say("failed to set keepalive: ", err)
    end
end

return _M
