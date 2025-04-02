---
-- clustersender.lua 是一个用于集群通信的Lua脚本。
-- 它主要负责与集群中的其他节点进行通信，包括发送请求和推送消息。
-- 该脚本使用了Skynet框架，通过socketchannel与远程节点建立连接，并处理请求和响应。
---
local skynet = require "skynet"
local sc = require "skynet.socketchannel"
local socket = require "skynet.socket"
local cluster = require "skynet.cluster.core"

local channel
local session = 1
local node, nodename, init_host, init_port = ...

local command = {}

-- 定义一个内部函数send_request，用于发送请求到指定地址
-- addr: 目标地址
-- msg: 请求消息
-- sz: 消息大小
local function send_request(addr, msg, sz)
    -- msg是一个本地指针，cluster.packrequest会释放它
    -- 保存当前的会话ID
    local current_session = session
    -- 调用cluster.packrequest函数，将请求消息打包，并获取新的会话ID和填充数据
    local request, new_session, padding = cluster.packrequest(addr, session, msg, sz)
    -- 更新会话ID为新的会话ID
    session = new_session
    -- 获取Skynet的跟踪标签
    local tracetag = skynet.tracetag()
    -- 如果存在跟踪标签
    if tracetag then
        -- 如果跟踪标签的第一个字符不是'('，则添加节点名和会话ID
        if tracetag:sub(1,1) ~= "(" then
            -- 格式化新的跟踪标签，包含节点名、节点地址和会话ID
            local newtag = string.format("(%s-%s-%d)%s", nodename, node, session, tracetag)
            -- 记录跟踪日志，包含原跟踪标签和新的会话标签
            skynet.tracelog(tracetag, string.format("session %s", newtag))
            -- 更新跟踪标签为新的标签
            tracetag = newtag
        end
        -- 记录跟踪日志，包含集群节点信息
        skynet.tracelog(tracetag, string.format("cluster %s", node))
        -- 通过channel发送跟踪标签请求
        channel:request(cluster.packtrace(tracetag))
    end
    -- 通过channel发送请求，并返回响应
    return channel:request(request, current_session, padding)
end

-- 定义command表中的req方法，用于处理请求
-- ...: 可变参数，传递给send_request函数
function command.req(...)
    -- 使用pcall调用send_request函数，捕获可能的错误
    local ok, msg = pcall(send_request, ...)
    -- 如果调用成功
    if ok then
        -- 如果返回的消息是一个表
        if type(msg) == "table" then
            -- 将表中的消息拼接成一个字符串，并返回给调用者
            skynet.ret(cluster.concat(msg))
        else
            -- 直接返回消息给调用者
            skynet.ret(msg)
        end
    else
        -- 如果调用失败，记录错误信息
        skynet.error(msg)
        -- 返回false给调用者，表示请求失败
        skynet.response()(false)
    end
end

-- 定义command表中的push方法，用于推送消息
-- addr: 目标地址
-- msg: 消息内容
-- sz: 消息大小
function command.push(addr, msg, sz)
    -- 调用cluster.packpush函数，将消息打包，并获取新的会话ID和填充数据
    local request, new_session, padding = cluster.packpush(addr, session, msg, sz)
    -- 如果存在填充数据，说明是多推送消息，更新会话ID
    if padding then    -- is multi push
        session = new_session
    end
    -- 通过channel发送请求，不等待响应
    channel:request(request, nil, padding)
end

-- 定义一个内部函数read_response，用于读取响应消息
-- sock: 套接字对象
local function read_response(sock)
    -- 从套接字中读取2个字节，解析出消息的大小
    local sz = socket.header(sock:read(2))
    -- 从套接字中读取指定大小的消息内容
    local msg = sock:read(sz)
    -- 调用cluster.unpackresponse函数，解包响应消息，返回会话ID、状态、数据和填充数据
    return cluster.unpackresponse(msg)    -- session, ok, data, padding
end

-- 定义command表中的changenode方法，用于更改连接的节点
-- host: 新的主机地址
-- port: 新的端口号
function command.changenode(host, port)
    -- 如果没有提供新的主机地址
    if not host then
        -- 记录错误信息，关闭当前的集群发送器
        skynet.error(string.format("Close cluster sender %s:%d", channel.__host, channel.__port))
        -- 关闭channel连接
        channel:close()
    else
        -- 更改channel的主机地址和端口号
        channel:changehost(host, tonumber(port))
        -- 重新连接到新的节点
        channel:connect(true)
    end
    -- 返回空消息给调用者
    skynet.ret(skynet.pack(nil))
end

skynet.start(function()
    -- 创建一个socketchannel实例，配置连接信息和响应处理函数
    channel = sc.channel {
            -- 初始主机地址
            host = init_host,
            -- 初始端口号
            port = tonumber(init_port),
            -- 响应处理函数
            response = read_response,
            -- 启用TCP_NODELAY选项
            nodelay = true,
        }
    -- 注册Lua消息处理器
    skynet.dispatch("lua", function(session , source, cmd, ...)
        -- 根据命令名查找对应的处理函数
        local f = assert(command[cmd])
        -- 调用处理函数，并传递参数
        f(...)
    end)
end)
