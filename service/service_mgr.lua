-- service_mgr.lua 文件的主要功能是管理服务的启动、查询和列表展示。它提供了全局和本地服务管理的功能，支持服务的全局和本地调用。

-- 引入 skynet 框架
local skynet = require "skynet"
-- 引入 skynet.manager 模块，用于注册服务
require "skynet.manager"    -- import skynet.register
-- 引入 skynet.snax 模块，用于处理 snax 服务
local snax = require "skynet.snax"

-- 定义一个空表，用于存储命令处理函数
local cmd = {}
-- 定义一个空表，用于存储服务信息
local service = {}

-- 定义 request 函数，用于请求服务并处理结果
local function request(name, func, ...)
    -- 调用 func 函数，并捕获可能的错误
    local ok, handle = pcall(func, ...)
    -- 获取服务名称对应的服务信息
    local s = service[name]
    -- 断言服务信息是一个表
    assert(type(s) == "table")
    -- 如果调用成功
    if ok then
        -- 将服务信息更新为服务句柄
        service[name] = handle
    else
        -- 将服务信息更新为错误信息的字符串表示
        service[name] = tostring(handle)
    end

    -- 遍历服务信息表中的所有等待协程
    for _,v in ipairs(s) do
        -- 唤醒等待的协程
        skynet.wakeup(v.co)
    end

    -- 如果调用成功
    if ok then
        -- 返回服务句柄
        return handle
    else
        -- 抛出错误信息
        error(tostring(handle))
    end
end

-- 定义 waitfor 函数，用于等待服务启动或获取服务句柄
local function waitfor(name , func, ...)
    -- 获取服务名称对应的服务信息
    local s = service[name]
    -- 如果服务信息是一个数字，表示服务已经启动，直接返回服务句柄
    if type(s) == "number" then
        return s
    end
    -- 获取当前协程
    local co = coroutine.running()

    -- 如果服务信息为空
    if s == nil then
        -- 创建一个新的服务信息表
        s = {}
        -- 将服务信息更新为新表
        service[name] = s
    -- 如果服务信息是一个字符串，表示服务启动失败，抛出错误信息
    elseif type(s) == "string" then
        error(s)
    end

    -- 断言服务信息是一个表
    assert(type(s) == "table")

    -- 获取当前会话和源地址
    local session, source = skynet.context()

    -- 如果服务信息中没有 launch 字段，并且传入了 func 函数
    if s.launch == nil and func then
        -- 设置 launch 字段，记录会话、源地址和协程信息
        s.launch = {
            session = session,
            source = source,
            co = co,
        }
        -- 调用 request 函数请求服务并返回结果
        return request(name, func, ...)
    end

    -- 将当前协程信息添加到服务信息表中
    table.insert(s, {
        co = co,
        session = session,
        source = source,
    })
    -- 让当前协程进入等待状态
    skynet.wait()
    -- 重新获取服务名称对应的服务信息
    s = service[name]
    -- 如果服务信息是一个字符串，表示服务启动失败，抛出错误信息
    if type(s) == "string" then
        error(s)
    end
    -- 断言服务信息是一个数字
    assert(type(s) == "number")
    -- 返回服务句柄
    return s
end

-- 定义 read_name 函数，用于处理服务名称，去除可能的 '@' 前缀
local function read_name(service_name)
    -- 如果服务名称的第一个字符是 '@'
    if string.byte(service_name) == 64 then -- '@'
        -- 返回去除 '@' 后的服务名称
        return string.sub(service_name , 2)
    else
        -- 直接返回服务名称
        return service_name
    end
end

-- 定义 cmd.LAUNCH 函数，用于启动服务
function cmd.LAUNCH(service_name, subname, ...)
    -- 调用 read_name 函数处理服务名称
    local realname = read_name(service_name)

    -- 如果服务名称是 'snaxd'
    if realname == "snaxd" then
        -- 调用 waitfor 函数启动 snax 服务，并返回服务句柄
        return waitfor(service_name.."."..subname, snax.rawnewservice, subname, ...)
    else
        -- 调用 waitfor 函数启动普通服务，并返回服务句柄
        return waitfor(service_name, skynet.newservice, realname, subname, ...)
    end
end

-- 定义 cmd.QUERY 函数，用于查询服务
function cmd.QUERY(service_name, subname)
    -- 调用 read_name 函数处理服务名称
    local realname = read_name(service_name)

    -- 如果服务名称是 'snaxd'
    if realname == "snaxd" then
        -- 调用 waitfor 函数查询 snax 服务，并返回服务句柄
        return waitfor(service_name.."."..subname)
    else
        -- 调用 waitfor 函数查询普通服务，并返回服务句柄
        return waitfor(service_name)
    end
end

-- 定义 list_service 函数，用于列出所有服务的信息
local function list_service()
    -- 定义一个空表，用于存储服务信息
    local result = {}
    -- 遍历 service 表中的所有服务信息
    for k,v in pairs(service) do
        -- 如果服务信息是一个字符串，表示服务启动失败
        if type(v) == "string" then
            -- 将服务信息更新为错误信息的字符串表示
            v = "Error: " .. v
        -- 如果服务信息是一个表，表示服务正在启动或查询中
        elseif type(v) == "table" then
            -- 定义一个空表，用于存储查询信息
            local querying = {}
            -- 如果服务信息中有 launch 字段，表示服务正在启动
            if v.launch then
                -- 获取启动协程的任务会话
                local session = skynet.task(v.launch.co)
                -- 调用 .launcher 服务查询启动地址
                local launching_address = skynet.call(".launcher", "lua", "QUERY", session)
                -- 如果启动地址存在
                if launching_address then
                    -- 将启动信息添加到查询信息表中
                    table.insert(querying, "Init as " .. skynet.address(launching_address))
                    -- 调用启动地址的 debug 接口查询初始化任务信息
                    table.insert(querying,  skynet.call(launching_address, "debug", "TASK", "init"))
                    -- 将启动源地址信息添加到查询信息表中
                    table.insert(querying, "Launching from " .. skynet.address(v.launch.source))
                    -- 调用启动源地址的 debug 接口查询启动会话任务信息
                    table.insert(querying, skynet.call(v.launch.source, "debug", "TASK", v.launch.session))
                end
            end
            -- 如果服务信息表中有等待协程
            if #v > 0 then
                -- 将查询信息添加到查询信息表中
                table.insert(querying , "Querying:" )
                -- 遍历所有等待协程
                for _, detail in ipairs(v) do
                    -- 将等待协程的源地址和任务信息添加到查询信息表中
                    table.insert(querying, skynet.address(detail.source) .. " " .. tostring(skynet.call(detail.source, "debug", "TASK", detail.session)))
                end
            end
            -- 将查询信息表转换为字符串
            v = table.concat(querying, "\n")
        else
            -- 将服务信息更新为服务地址的字符串表示
            v = skynet.address(v)
        end

        -- 将服务名称和服务信息添加到结果表中
        result[k] = v
    end

    -- 返回结果表
    return result
end


-- 定义 register_global 函数，用于注册全局服务管理命令
local function register_global()
    -- 定义 cmd.GLAUNCH 函数，用于全局启动服务
    function cmd.GLAUNCH(name, ...)
        -- 构建全局服务名称
        local global_name = "@" .. name
        -- 调用 cmd.LAUNCH 函数启动全局服务，并返回服务句柄
        return cmd.LAUNCH(global_name, ...)
    end

    -- 定义 cmd.GQUERY 函数，用于全局查询服务
    function cmd.GQUERY(name, ...)
        -- 构建全局服务名称
        local global_name = "@" .. name
        -- 调用 cmd.QUERY 函数查询全局服务，并返回服务句柄
        return cmd.QUERY(global_name, ...)
    end

    -- 定义一个空表，用于存储报告的服务管理器
    local mgr = {}

    -- 定义 cmd.REPORT 函数，用于报告服务管理器
    function cmd.REPORT(m)
        -- 将服务管理器添加到 mgr 表中
        mgr[m] = true
    end

    -- 定义 add_list 函数，用于将其他服务管理器的服务信息添加到结果表中
    local function add_list(all, m)
        -- 构建服务管理器的 harbor 名称
        local harbor = "@" .. skynet.harbor(m)
        -- 调用服务管理器的 LIST 接口获取服务信息
        local result = skynet.call(m, "lua", "LIST")
        -- 遍历服务信息表
        for k,v in pairs(result) do
            -- 将服务名称和服务信息添加到结果表中，并添加 harbor 名称
            all[k .. harbor] = v
        end
    end

    -- 定义 cmd.LIST 函数，用于列出所有全局服务信息
    function cmd.LIST()
        -- 定义一个空表，用于存储结果信息
        local result = {}
        -- 遍历 mgr 表中的所有服务管理器
        for k in pairs(mgr) do
            -- 调用 add_list 函数将服务管理器的服务信息添加到结果表中
            pcall(add_list, result, k)
        end
        -- 调用 list_service 函数获取本地服务信息
        local l = list_service()
        -- 遍历本地服务信息表
        for k, v in pairs(l) do
            -- 将本地服务信息添加到结果表中
            result[k] = v
        end
        -- 返回结果表
        return result
    end
end

-- 定义 register_local 函数，用于注册本地服务管理命令
local function register_local()
    -- 定义 waitfor_remote 函数，用于等待远程服务启动或查询
    local function waitfor_remote(cmd, name, ...)
        -- 构建全局服务名称
        local global_name = "@" .. name
        -- 定义本地服务名称
        local local_name
        -- 如果服务名称是 'snaxd'
        if name == "snaxd" then
            -- 构建本地 snax 服务名称
            local_name = global_name .. "." .. (...)
        else
            -- 构建本地普通服务名称
            local_name = global_name
        end
        -- 调用 waitfor 函数等待远程服务启动或查询，并返回服务句柄
        return waitfor(local_name, skynet.call, "SERVICE", "lua", cmd, global_name, ...)
    end

    -- 定义 cmd.GLAUNCH 函数，用于本地全局启动服务
    function cmd.GLAUNCH(...)
        -- 调用 waitfor_remote 函数启动远程服务，并返回服务句柄
        return waitfor_remote("LAUNCH", ...)
    end

    -- 定义 cmd.GQUERY 函数，用于本地全局查询服务
    function cmd.GQUERY(...)
        -- 调用 waitfor_remote 函数查询远程服务，并返回服务句柄
        return waitfor_remote("QUERY", ...)
    end

    -- 定义 cmd.LIST 函数，用于列出所有本地服务信息
    function cmd.LIST()
        -- 调用 list_service 函数获取本地服务信息
        return list_service()
    end

    -- 调用 SERVICE 服务报告本地服务管理器
    skynet.call("SERVICE", "lua", "REPORT", skynet.self())
end

-- 启动 skynet 服务
skynet.start(function()
    -- 定义 lua 消息的处理函数
    skynet.dispatch("lua", function(session, address, command, ...)
        -- 获取命令对应的处理函数
        local f = cmd[command]
        -- 如果处理函数不存在
        if f == nil then
            -- 返回错误信息
            skynet.ret(skynet.pack(nil, "Invalid command " .. command))
            return
        end

        -- 调用处理函数，并捕获可能的错误
        local ok, r = pcall(f, ...)

        -- 如果调用成功
        if ok then
            -- 返回处理结果
            skynet.ret(skynet.pack(r))
        else
            -- 返回错误信息
            skynet.ret(skynet.pack(nil, r))
        end
    end)
    -- 获取 .service 服务的句柄
    local handle = skynet.localname ".service"
    -- 如果 .service 服务已经注册
    if  handle then
        -- 输出错误信息
        skynet.error(".service is already register by ", skynet.address(handle))
        -- 退出服务
        skynet.exit()
    else
        -- 注册 .service 服务
        skynet.register(".service")
    end
    -- 如果 skynet 环境变量中 standalone 为真
    if skynet.getenv "standalone" then
        -- 注册 SERVICE 服务
        skynet.register("SERVICE")
        -- 调用 register_global 函数注册全局服务管理命令
        register_global()
    else
        -- 调用 register_local 函数注册本地服务管理命令
        register_local()
    end
end)
