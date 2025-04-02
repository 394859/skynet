-- 引入skynet框架
local skynet = require "skynet"
-- 引入gateserver模块
local gateserver = require "snax.gateserver"

-- 看门狗服务地址
local watchdog
-- 连接表，存储每个连接的信息，键为文件描述符，值为连接信息表
local connection = {} -- fd -> connection : { fd , client, agent , ip, mode }

-- 注册一个名为client的协议
skynet.register_protocol {
    -- 协议名称
    name = "client",
    -- 协议ID
    id = skynet.PTYPE_CLIENT,
}

-- 定义一个处理程序表，用于处理各种事件
local handler = {}

-- 处理服务启动事件的函数
function handler.open(source, conf)
    -- 设置看门狗服务地址，若配置中未指定，则使用源地址
    watchdog = conf.watchdog or source
    -- 返回配置中的地址和端口
    return conf.address, conf.port
end

-- 处理接收到客户端消息事件的函数
function handler.message(fd, msg, sz)
    -- 从连接表中获取该文件描述符对应的连接信息
    local c = connection[fd]
    -- 获取该连接对应的代理服务地址
    local agent = c.agent
    if agent then
        -- 如果存在代理服务，直接将消息重定向到代理服务
        -- gateserver框架不会释放消息，因此可以直接重定向
        skynet.redirect(agent, c.client, "client", fd, msg, sz)
    else
        -- 如果不存在代理服务，将消息发送给看门狗服务
        skynet.send(watchdog, "lua", "socket", "data", fd, skynet.tostring(msg, sz))
        -- skynet.tostring会将消息复制到一个字符串中，因此需要释放原始消息
        skynet.trash(msg,sz)
    end
end

-- 处理客户端连接事件的函数
function handler.connect(fd, addr)
    -- 创建一个新的连接信息表
    local c = {
        -- 文件描述符
        fd = fd,
        -- 客户端IP地址
        ip = addr,
    }
    -- 将连接信息存储到连接表中
    connection[fd] = c
    -- 向看门狗服务发送连接打开消息
    skynet.send(watchdog, "lua", "socket", "open", fd, addr)
end

-- 取消消息转发的辅助函数
local function unforward(c)
    if c.agent then
        -- 清除代理服务地址
        c.agent = nil
        -- 清除客户端地址
        c.client = nil
    end
end

-- 关闭文件描述符对应的连接的辅助函数
local function close_fd(fd)
    -- 从连接表中获取该文件描述符对应的连接信息
    local c = connection[fd]
    if c then
        -- 取消该连接的消息转发
        unforward(c)
        -- 从连接表中移除该连接信息
        connection[fd] = nil
    end
end

-- 处理客户端断开连接事件的函数
function handler.disconnect(fd)
    -- 关闭该文件描述符对应的连接
    close_fd(fd)
    -- 向看门狗服务发送连接关闭消息
    skynet.send(watchdog, "lua", "socket", "close", fd)
end

-- 处理客户端连接错误事件的函数
function handler.error(fd, msg)
    -- 关闭该文件描述符对应的连接
    close_fd(fd)
    -- 向看门狗服务发送连接错误消息
    skynet.send(watchdog, "lua", "socket", "error", fd, msg)
end

-- 处理客户端连接警告事件的函数
function handler.warning(fd, size)
    -- 向看门狗服务发送连接警告消息
    skynet.send(watchdog, "lua", "socket", "warning", fd, size)
end

-- 定义一个命令表，用于处理各种命令
local CMD = {}

-- 处理forward命令的函数，用于将客户端连接转发到指定代理服务
function CMD.forward(source, fd, client, address)
    -- 从连接表中获取该文件描述符对应的连接信息
    local c = assert(connection[fd])
    -- 取消该连接的消息转发
    unforward(c)
    -- 设置客户端地址
    c.client = client or 0
    -- 设置代理服务地址
    c.agent = address or source
    -- 打开该客户端连接
    gateserver.openclient(fd)
end

-- 处理accept命令的函数，用于接受客户端连接
function CMD.accept(source, fd)
    -- 从连接表中获取该文件描述符对应的连接信息
    local c = assert(connection[fd])
    -- 取消该连接的消息转发
    unforward(c)
    -- 打开该客户端连接
    gateserver.openclient(fd)
end

-- 处理kick命令的函数，用于踢掉客户端连接
function CMD.kick(source, fd)
    -- 关闭该客户端连接
    gateserver.closeclient(fd)
end

-- 处理命令事件的函数
function handler.command(cmd, source, ...)
    -- 根据命令名称从命令表中获取对应的处理函数
    local f = assert(CMD[cmd])
    -- 执行处理函数并返回结果
    return f(source, ...)
end

-- 启动gateserver服务，传入处理程序表
gateserver.start(handler)
