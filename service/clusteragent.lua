-- 引入skynet框架
local skynet = require "skynet"
-- 引入socket模块
local socket = require "skynet.socket"
-- 引入集群核心模块
local cluster = require "skynet.cluster.core"
-- 忽略返回值的函数
local ignoreret = skynet.ignoreret

-- 获取传入的参数，分别为clusterd、gate和fd，并转换为数字类型
local clusterd, gate, fd = ...
clusterd = tonumber(clusterd)
gate = tonumber(gate)
fd = tonumber(fd)

-- 存储大请求的表
local large_request = {}
-- 存储正在查询的名称的表
local inquery_name = {}
-- 存储注册名称的表
local register_name

-- 注册名称的元表，用于处理名称查询时的逻辑
local register_name_mt = { __index =
    function(self, name)
        -- 检查是否有协程正在等待该名称的查询结果
        local waitco = inquery_name[name]
        if waitco then
            -- 获取当前协程
            local co = coroutine.running()
            -- 将当前协程添加到等待队列中
            table.insert(waitco, co)
            -- 挂起当前协程
            skynet.wait(co)
            -- 返回注册名称表中该名称对应的值
            return rawget(register_name, name)
        else
            -- 创建一个新的等待队列
            waitco = {}
            -- 将等待队列存储到正在查询的名称表中
            inquery_name[name] = waitco

            -- 调用clusterd服务查询名称对应的地址
            local addr = skynet.call(clusterd, "lua", "queryname", name:sub(2)) -- name must be '@xxxx'
            if addr then
                -- 如果查询到地址，将地址存储到注册名称表中
                register_name[name] = addr
            end
            -- 从正在查询的名称表中移除该名称的等待队列
            inquery_name[name] = nil
            -- 唤醒所有等待该名称查询结果的协程
            for _, co in ipairs(waitco) do
                skynet.wakeup(co)
            end
            -- 返回查询到的地址
            return addr
        end
    end
}

-- 创建一个新的注册名称表
local function new_register_name()
    register_name = setmetatable({}, register_name_mt)
end
-- 初始化注册名称表
new_register_name()

-- 跟踪标签
local tracetag

-- 处理客户端请求的函数
local function dispatch_request(_,_,addr, session, msg, sz, padding, is_push)
    -- 忽略返回值
    ignoreret() -- session is fd, don't call skynet.ret
    if session == nil then
        -- 如果session为nil，设置跟踪标签
        tracetag = addr
        return
    end
    if padding then
        -- 如果有填充数据，说明是大请求的一部分
        local req = large_request[session] or { addr = addr , is_push = is_push, tracetag = tracetag }
        tracetag = nil
        -- 将请求存储到大型请求表中
        large_request[session] = req
        -- 追加数据到请求中
        cluster.append(req, msg, sz)
        return
    else
        -- 如果没有填充数据，说明是大请求的最后一部分或普通请求
        local req = large_request[session]
        if req then
            -- 如果是大请求的最后一部分，恢复跟踪标签
            tracetag = req.tracetag
            -- 从大型请求表中移除该请求
            large_request[session] = nil
            -- 追加数据到请求中
            cluster.append(req, msg, sz)
            -- 拼接请求数据
            msg,sz = cluster.concat(req)
            -- 获取请求的地址
            addr = req.addr
            -- 获取请求是否为推送类型
            is_push = req.is_push
        end
        if not msg then
            -- 如果没有有效的消息，设置跟踪标签为nil
            tracetag = nil
            -- 打包响应消息，返回无效大请求的错误信息
            local response = cluster.packresponse(session, false, "Invalid large req")
            -- 向客户端发送响应消息
            socket.write(fd, response)
            return
        end
    end
    local ok, response
    if addr == 0 then
        -- 如果地址为0，说明是查询名称的请求
        local name = skynet.unpack(msg, sz)
        -- 释放消息资源
        skynet.trash(msg, sz)
        -- 从注册名称表中获取名称对应的地址
        local addr = register_name["@" .. name]
        if addr then
            -- 如果查询到地址，设置ok为true，并打包地址为消息
            ok = true
            msg = skynet.packstring(addr)
        else
            -- 如果未查询到地址，设置ok为false，并设置错误消息
            ok = false
            msg = "name not found"
        end
        -- 设置消息大小为nil
        sz = nil
    else
        -- 如果地址不为0，说明是普通请求
        if cluster.isname(addr) then
            -- 如果地址是名称，从注册名称表中获取对应的地址
            addr = register_name[addr]
        end
        if addr then
            -- 如果地址有效
            if is_push then
                -- 如果是推送类型的请求，直接发送消息到目标地址
                skynet.rawsend(addr, "lua", msg, sz)
                return -- no response
            else
                if tracetag then
                    -- 如果有跟踪标签，使用跟踪调用发送消息到目标地址
                    ok , msg, sz = pcall(skynet.tracecall, tracetag, addr, "lua", msg, sz)
                    tracetag = nil
                else
                    -- 如果没有跟踪标签，使用原始调用发送消息到目标地址
                    ok , msg, sz = pcall(skynet.rawcall, addr, "lua", msg, sz)
                end
            end
        else
            -- 如果地址无效，设置ok为false，并设置错误消息
            ok = false
            msg = "Invalid name"
        end
    end
    if ok then
        -- 如果请求处理成功，打包响应消息
        response = cluster.packresponse(session, true, msg, sz)
        if type(response) == "table" then
            -- 如果响应消息是表类型，逐行发送响应消息
            for _, v in ipairs(response) do
                socket.lwrite(fd, v)
            end
        else
            -- 如果响应消息是字符串类型，直接发送响应消息
            socket.write(fd, response)
        end
    else
        -- 如果请求处理失败，打包错误响应消息
        response = cluster.packresponse(session, false, msg)
        -- 发送错误响应消息
        socket.write(fd, response)
    end
end

-- 启动skynet服务
skynet.start(function()
    -- 注册客户端协议
    skynet.register_protocol {
        name = "client",
        id = skynet.PTYPE_CLIENT,
        unpack = cluster.unpackrequest,
        dispatch = dispatch_request,
    }
    -- 尝试调用gate服务将fd进行转发
    -- forward may fail, see https://github.com/cloudwu/skynet/issues/1958
    pcall(skynet.call,gate, "lua", "forward", fd)

    -- 处理lua类型的消息
    skynet.dispatch("lua", function(_,source, cmd, ...)
        if cmd == "exit" then
            -- 如果命令是exit，关闭fd并退出服务
            socket.close_fd(fd)
            skynet.exit()
        elseif cmd == "namechange" then
            -- 如果命令是namechange，创建一个新的注册名称表
            new_register_name()
        else
            -- 如果是其他命令，记录错误信息
            skynet.error(string.format("Invalid command %s from %s", cmd, skynet.address(source)))
        end
    end)
end)
