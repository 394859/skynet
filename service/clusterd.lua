-- 引入skynet库
local skynet = require "skynet"
 -- 引入skynet.manager模块
require "skynet.manager"
 -- 引入skynet.cluster.core模块
local cluster = require "skynet.cluster.core"

 -- 从环境变量中获取集群配置文件的名称
local config_name = skynet.getenv "cluster"
 -- 存储节点地址的表
local node_address = {}
 -- 存储节点发送器的表
local node_sender = {}
 -- 存储节点发送器关闭状态的表
local node_sender_closed = {}
 -- 存储命令处理函数的表
local command = {}
 -- 存储配置信息的表
local config = {}
 -- 获取当前节点的名称
local nodename = cluster.nodename()

 -- 存储正在连接的节点信息的表
local connecting = {}

 -- 打开一个到指定节点的通道
local function open_channel(t, key)
    -- 获取正在连接的节点信息
    local ct = connecting[key]
    if ct then
        -- 获取当前协程
        local co = coroutine.running()
        local channel
        while ct do
            -- 将当前协程加入等待列表
            table.insert(ct, co)
            -- 挂起当前协程
            skynet.wait(co)
            -- 获取通道信息
            channel = ct.channel
            ct = connecting[key]
            -- 如果ct不为空，重新加载
        end
        -- 断言节点地址和通道存在
        return assert(node_address[key] and channel)
    end
    ct = {}
    -- 记录正在连接的节点信息
    connecting[key] = ct
    -- 获取节点地址
    local address = node_address[key]
    if address == nil and not config.nowaiting then
        -- 获取当前协程
        local co = coroutine.running()
        -- 断言namequery字段为空
        assert(ct.namequery == nil)
        ct.namequery = co
        -- 输出等待节点的信息
        skynet.error("Waiting for cluster node [".. key.."]")
        -- 挂起当前协程
        skynet.wait(co)
        -- 获取节点地址
        address = node_address[key]
    end
    local succ, err, c
    if address then
        -- 从地址中提取主机和端口
        local host, port = string.match(address, "([^:]+):(.*)$")
        -- 获取节点发送器
        c = node_sender[key]
        if c == nil then
            -- 创建一个新的clustersender服务
            c = skynet.newservice("clustersender", key, nodename, host, port)
            if node_sender[key] then
                -- 双重检查，避免重复创建
                skynet.kill(c)
                c = node_sender[key]
            else
                -- 记录节点发送器
                node_sender[key] = c
            end
        end

        -- 调用节点发送器的changenode方法
        succ = pcall(skynet.call, c, "lua", "changenode", host, port)

        if succ then
            -- 记录通道信息
            t[key] = c
            ct.channel = c
            -- 标记节点发送器未关闭
            node_sender_closed[key] = nil
        else
            -- 输出changenode失败的信息
            err = string.format("changenode [%s] (%s:%s) failed", key, host, port)
        end
    elseif address == false then
        -- 获取节点发送器
        c = node_sender[key]
        if c == nil or node_sender_closed[key] then
            -- 没有发送器或已关闭，总是成功
            succ = true
        else
            -- 关闭发送器
            succ, err = pcall(skynet.call, c, "lua", "changenode", false)
            if succ then -- 关闭失败，等待下一次尝试关闭
                node_sender_closed[key] = true
            end
        end
    else
        -- 输出节点不存在的信息
        err = string.format("cluster node [%s] is absent.", key)
    end
    -- 清除正在连接的节点信息
    connecting[key] = nil
    -- 唤醒所有等待的协程
    for _, co in ipairs(ct) do
        skynet.wakeup(co)
    end
    if node_address[key] ~= address then
        -- 如果节点地址发生变化，重新打开通道
        return open_channel(t,key)
    end
    -- 断言操作成功
    assert(succ, err)
    return c
end

 -- 创建一个元表，用于在访问不存在的键时调用open_channel函数
local node_channel = setmetatable({}, { __index = open_channel })

 -- 加载集群配置
local function loadconfig(tmp)
    if tmp == nil then
        tmp = {}
        if config_name then
            -- 打开配置文件
            local f = assert(io.open(config_name))
            -- 读取文件内容
            local source = f:read "*a"
            -- 关闭文件
            f:close()
            -- 加载配置文件内容
            assert(load(source, "@"..config_name, "t", tmp))()
        end
    end
    local reload = {}
    for name,address in pairs(tmp) do
        if name:sub(1,2) == "__" then
            -- 处理配置项
            name = name:sub(3)
            config[name] = address
            -- 输出配置信息
            skynet.error(string.format("Config %s = %s", name, address))
        else
            -- 断言地址类型为false或字符串
            assert(address == false or type(address) == "string")
            if node_address[name] ~= address then
                -- 地址发生变化
                if node_sender[name] then
                    -- 如果节点发送器存在，重置连接
                    node_channel[name] = nil
                    -- 将需要重新加载的节点名称加入列表
                    table.insert(reload, name)
                end
                -- 更新节点地址
                node_address[name] = address
            end
            -- 获取正在连接的节点信息
            local ct = connecting[name]
            if ct and ct.namequery and not config.nowaiting then
                -- 输出节点解析信息
                skynet.error(string.format("Cluster node [%s] resloved : %s", name, address))
                -- 唤醒等待的协程
                skynet.wakeup(ct.namequery)
            end
        end
    end
    if config.nowaiting then
        -- 唤醒所有正在连接的请求
        for name, ct in pairs(connecting) do
            if ct.namequery then
                skynet.wakeup(ct.namequery)
            end
        end
    end
    for _, name in ipairs(reload) do
        -- 异步打开通道
        skynet.fork(open_channel, node_channel, name)
    end
end

 -- 处理reload命令
function command.reload(source, config)
    -- 加载配置
    loadconfig(config)
    -- 返回空结果
    skynet.ret(skynet.pack(nil))
end

 -- 处理listen命令
function command.listen(source, addr, port, maxclient)
    -- 创建一个gate服务
    local gate = skynet.newservice("gate")
    if port == nil then
        -- 获取节点地址
        local address = assert(node_address[addr], addr .. " is down")
        -- 从地址中提取主机和端口
        addr, port = string.match(address, "(.+):([^:]+)$")
        port = tonumber(port)
        -- 断言端口不为0
        assert(port ~= 0)
        -- 调用gate服务的open方法
        skynet.call(gate, "lua", "open", { address = addr, port = port, maxclient = maxclient })
        -- 返回主机和端口
        skynet.ret(skynet.pack(addr, port))
    else
        -- 调用gate服务的open方法
        local realaddr, realport = skynet.call(gate, "lua", "open", { address = addr, port = port, maxclient = maxclient })
        -- 返回实际的主机和端口
        skynet.ret(skynet.pack(realaddr, realport))
    end
end

 -- 处理sender命令
function command.sender(source, node)
    -- 返回节点通道
    skynet.ret(skynet.pack(node_channel[node]))
end

 -- 处理senders命令
function command.senders(source)
    -- 返回所有节点发送器
    skynet.retpack(node_sender)
end

 -- 存储代理服务的表
local proxy = {}

 -- 处理proxy命令
function command.proxy(source, node, name)
    if name == nil then
        -- 从节点名称中提取节点和服务名称
        node, name = node:match "^([^@.]+)([@.].+)"
        if name == nil then
            -- 抛出无效名称的错误
            error ("Invalid name " .. tostring(node))
        end
    end
    -- 生成完整的服务名称
    local fullname = node .. "." .. name
    -- 获取代理服务
    local p = proxy[fullname]
    if p == nil then
        -- 创建一个新的clusterproxy服务
        p = skynet.newservice("clusterproxy", node, name)
        -- 双重检查，避免重复创建
        if proxy[fullname] then
            skynet.kill(p)
            p = proxy[fullname]
        else
            -- 记录代理服务
            proxy[fullname] = p
        end
    end
    -- 返回代理服务
    skynet.ret(skynet.pack(p))
end

 -- 存储集群代理服务的表，键为文件描述符，值为服务
local cluster_agent = {}	-- fd:service
 -- 存储注册名称的表，键为名称，值为地址
local register_name = {}

 -- 清除名称缓存
local function clearnamecache()
    for fd, service in pairs(cluster_agent) do
        if type(service) == "number" then
            -- 发送namechange消息给服务
            skynet.send(service, "lua", "namechange")
        end
    end
end

 -- 处理register命令
function command.register(source, name, addr)
    -- 断言名称未被注册
    assert(register_name[name] == nil)
    -- 如果地址为空，使用源地址
    addr = addr or source
    -- 获取旧的名称
    local old_name = register_name[addr]
    if old_name then
        -- 清除旧名称的注册信息
        register_name[old_name] = nil
        -- 清除名称缓存
        clearnamecache()
    end
    -- 记录名称和地址的映射
    register_name[addr] = name
    register_name[name] = addr
    -- 返回空结果
    skynet.ret(nil)
    -- 输出注册信息
    skynet.error(string.format("Register [%s] :%08x", name, addr))
end

 -- 处理unregister命令
function command.unregister(_, name)
    if not register_name[name] then
        -- 如果名称未注册，直接返回空结果
        return skynet.ret(nil)
    end
    -- 获取地址
    local addr = register_name[name]
    -- 清除地址的注册信息
    register_name[addr] = nil
    -- 清除名称的注册信息
    register_name[name] = nil
    -- 清除名称缓存
    clearnamecache()
    -- 返回空结果
    skynet.ret(nil)
    -- 输出注销信息
    skynet.error(string.format("Unregister [%s] :%08x", name, addr))
end

 -- 处理queryname命令
function command.queryname(source, name)
    -- 返回名称对应的地址
    skynet.ret(skynet.pack(register_name[name]))
end

 -- 处理socket命令
function command.socket(source, subcmd, fd, msg)
    if subcmd == "open" then
        -- 输出socket连接信息
        skynet.error(string.format("socket accept from %s", msg))
        -- 标记新的集群代理服务
        cluster_agent[fd] = false
        -- 创建一个新的clusteragent服务
        local agent = skynet.newservice("clusteragent", skynet.self(), source, fd)
        -- 获取关闭状态
        local closed = cluster_agent[fd]
        -- 记录集群代理服务
        cluster_agent[fd] = agent
        if closed then
            -- 如果已关闭，发送exit消息给服务
            skynet.send(agent, "lua", "exit")
            -- 清除集群代理服务记录
            cluster_agent[fd] = nil
        end
    else
        if subcmd == "close" or subcmd == "error" then
            -- 获取集群代理服务
            local agent = cluster_agent[fd]
            if type(agent) == "boolean" then
                -- 标记为已关闭
                cluster_agent[fd] = true
            elseif agent then
                -- 发送exit消息给服务
                skynet.send(agent, "lua", "exit")
                -- 清除集群代理服务记录
                cluster_agent[fd] = nil
            end
        else
            -- 输出socket消息信息
            skynet.error(string.format("socket %s %d %s", subcmd, fd, msg or ""))
        end
    end
end

 -- 启动skynet服务
skynet.start(function()
    -- 加载配置
    loadconfig()
    -- 注册lua消息处理函数
    skynet.dispatch("lua", function(session , source, cmd, ...)
        -- 获取命令处理函数
        local f = assert(command[cmd])
        -- 调用命令处理函数
        f(source, ...)
    end)
end)
