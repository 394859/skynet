--- 引入skynet库
local skynet = require "skynet"

--- 定义命令表，用于存储各种命令处理函数
local command = {}
--- 定义数据库表，用于存储数据
local database = {}
--- 定义等待队列表，用于存储等待的请求
local wait_queue = {}
--- 定义模式表，用于标记队列的模式
local mode = {}

--- 递归查询数据库中的值
--- @param db table 要查询的数据库表
--- @param key any 要查询的键
--- @param ... any 可选的后续键，用于递归查询
--- @return any 返回查询到的值，如果未找到则返回nil
local function query(db, key, ...)
    -- 如果数据库表或键为nil，则直接返回数据库表
    if db == nil or key == nil then
        return db
    else
        -- 递归查询数据库表中键对应的值
        return query(db[key], ...)
    end
end

--- 处理QUERY命令，查询数据库中的值
--- @param key any 要查询的键
--- @param ... any 可选的后续键，用于递归查询
--- @return any 返回查询到的值，如果未找到则返回nil
function command.QUERY(key, ...)
    -- 获取数据库中指定键的值
    local d = database[key]
    -- 如果值不为nil，则进行递归查询
    if d ~= nil then
        return query(d, ...)
    end
end

--- 递归更新数据库中的值
--- @param db table 要更新的数据库表
--- @param key any 要更新的键
--- @param value any 要更新的值
--- @param ... any 可选的后续键，用于递归更新
--- @return any 返回更新前的值和更新后的值
local function update(db, key, value, ...)
    -- 如果没有后续键，则直接更新数据库表中键对应的值
    if select("#",...) == 0 then
        local ret = db[key]
        db[key] = value
        return ret, value
    else
        -- 如果数据库表中键对应的值为nil，则创建一个新的表
        if db[key] == nil then
            db[key] = {}
        end
        -- 递归更新数据库表中键对应的值
        return update(db[key], value, ...)
    end
end

--- 唤醒等待队列中的请求
--- @param db table 要操作的等待队列表
--- @param key1 any 要操作的键
--- @param ... any 可选的后续键，用于递归操作
--- @return table|nil 返回唤醒的队列，如果未找到则返回nil
local function wakeup(db, key1, ...)
    -- 如果键为nil，则直接返回
    if key1 == nil then
        return
    end
    -- 获取等待队列表中指定键的值
    local q = db[key1]
    -- 如果值为nil，则直接返回
    if q == nil then
        return
    end
    -- 如果队列的模式为"queue"，则进行相应的处理
    if q[mode] == "queue" then
        -- 移除队列
        db[key1] = nil
        -- 如果有多个后续键，则抛出错误，因为不能唤醒一个分支
        if select("#", ...) ~= 1 then
            -- 遍历队列中的每个响应，调用响应函数并传入false
            for _,response in ipairs(q) do
                response(false)
            end
        else
            -- 返回队列
            return q
        end
    else
        -- 如果是分支，则递归唤醒分支中的请求
        return wakeup(q , ...)
    end
end

--- 处理UPDATE命令，更新数据库中的值，并唤醒等待队列中的请求
--- @param ... any 要更新的键和值
--- @return any 返回更新前的值，如果没有更新则返回nil
function command.UPDATE(...)
    -- 调用update函数更新数据库中的值
    local ret, value = update(database, ...)
    -- 如果更新前的值不为nil或更新后的值为nil，则直接返回更新前的值
    if ret ~= nil or value == nil then
        return ret
    end
    -- 调用wakeup函数唤醒等待队列中的请求
    local q = wakeup(wait_queue, ...)
    -- 如果唤醒了队列，则遍历队列中的每个响应，调用响应函数并传入true和更新后的值
    if q then
        for _, response in ipairs(q) do
            response(true,value)
        end
    end
end

--- 将请求加入等待队列
--- @param db table 要操作的等待队列表
--- @param key1 any 要操作的键
--- @param key2 any 可选的后续键，用于递归操作
--- @param ... any 可选的后续键，用于递归操作
local function waitfor(db, key1, key2, ...)
    -- 如果没有后续键，则将请求加入队列
    if key2 == nil then
        -- 获取等待队列表中指定键的值
        local q = db[key1]
        -- 如果值为nil，则创建一个新的队列
        if q == nil then
            q = { [mode] = "queue" }
            db[key1] = q
        else
            -- 断言队列的模式为"queue"
            assert(q[mode] == "queue")
        end
        -- 将当前请求的响应加入队列
        table.insert(q, skynet.response())
    else
        -- 获取等待队列表中指定键的值
        local q = db[key1]
        -- 如果值为nil，则创建一个新的分支
        if q == nil then
            q = { [mode] = "branch" }
            db[key1] = q
        else
            -- 断言队列的模式为"branch"
            assert(q[mode] == "branch")
        end
        -- 递归将请求加入分支中的队列
        return waitfor(q, key2, ...)
    end
end

--- 启动skynet服务
skynet.start(function()
    -- 注册lua消息处理函数
    skynet.dispatch("lua", function (_, _, cmd, ...)
        -- 如果命令为"WAIT"，则进行相应的处理
        if cmd == "WAIT" then
            -- 调用command.QUERY函数查询数据库中的值
            local ret = command.QUERY(...)
            -- 如果查询到的值不为nil，则返回查询结果
            if ret ~= nil then
                skynet.ret(skynet.pack(ret))
            else
                -- 将请求加入等待队列
                waitfor(wait_queue, ...)
            end
        else
            -- 获取命令对应的处理函数
            local f = assert(command[cmd])
            -- 调用处理函数并返回结果
            skynet.ret(skynet.pack(f(...)))
        end
    end)
end)
