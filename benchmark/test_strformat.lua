queue = 'queue1'
local receiver = 'receiver_1'
function test(n)
    local sql1 = string.format('select * from %s_msg where id>=%d limit %d', queue, n, 1)
    local result = {{id=n}}
    local values = {}
    for i, message in ipairs(result) do
        table.insert(values, string.format("(%d, '%s')", message['id'], receiver))
    end
    local sql2 = string.format('insert into %s_rst(m_id, receiver) values%s', queue, table.concat(values, ','))
end

s = os.clock()
for n =1,5000000 do
    test(n)
end
print(os.clock()-s)

-- 500w
-- lua5.3 14s
-- luajit-2.0.5 22s
-- luajit-2.1.0-beta3 9s
