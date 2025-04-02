---
-- @file cmaster.lua
-- @brief 该文件实现了一个主节点管理服务，用于管理从节点信息和全局名称映射。
-- 主节点接收来自从节点的连接，处理从节点的注册、查询请求，并在从节点状态变化时通知其他从节点。
--
-- @author Your Name
-- @date 2024-07-25
---
local skynet = require "skynet"
local socket = require "skynet.socket"

--[[
    master manage data :
        1. all the slaves address : id -> ipaddr:port
        2. all the global names : name -> address

    master hold connections from slaves .

    protocol slave->master :
        package size 1 byte
        type 1 byte :
            'H' : HANDSHAKE, report slave id, and address.
            'R' : REGISTER name address
            'Q' : QUERY name


    protocol master->slave:
        package size 1 byte
        type 1 byte :
            'W' : WAIT n
            'C' : CONNECT slave_id slave_address
            'N' : NAME globalname address
            'D' : DISCONNECT slave_id
]]

-- 存储从节点信息的表，键为从节点ID，值为包含从节点文件描述符、ID和地址的表
local slave_node = {}
-- 存储全局名称映射的表，键为全局名称，值为对应的地址
local global_name = {}

-- 从指定文件描述符读取一个数据包
-- @param fd 文件描述符
-- @return 解包后的数据包内容
local function read_package(fd)
    -- 读取数据包大小（1字节）
    local sz = socket.read(fd, 1)
    -- 确保读取成功，否则抛出错误
    assert(sz, "closed")
    -- 将字节转换为整数
    sz = string.byte(sz)
    -- 读取指定大小的数据包内容
    local content = assert(socket.read(fd, sz), "closed")
    -- 解包数据包内容
    return skynet.unpack(content)
end

-- 打包一个数据包
-- @param ... 要打包的内容
-- @return 打包后的数据包，包含数据包大小和内容
local function pack_package(...)
    -- 打包内容为字符串
    local message = skynet.packstring(...)
    -- 获取打包后字符串的长度
    local size = #message
    -- 确保数据包长度不超过255字节
    assert(size <= 255 , "too long")
    -- 将数据包大小和内容拼接成一个字符串
    return string.char(size) .. message
end

-- 向所有从节点报告新从节点的信息，并通知当前从节点等待的从节点数量
-- @param fd 当前从节点的文件描述符
-- @param slave_id 新从节点的ID
-- @param slave_addr 新从节点的地址
local function report_slave(fd, slave_id, slave_addr)
    -- 打包一个CONNECT消息，包含新从节点的ID和地址
    local message = pack_package("C", slave_id, slave_addr)
    -- 初始化等待的从节点数量为0
    local n = 0
    -- 遍历所有从节点
    for k,v in pairs(slave_node) do
        -- 如果从节点的文件描述符不为0（表示连接正常）
        if v.fd ~= 0 then
            -- 向该从节点发送CONNECT消息
            socket.write(v.fd, message)
            -- 增加等待的从节点数量
            n = n + 1
        end
    end
    -- 向当前从节点发送WAIT消息，通知其等待的从节点数量
    socket.write(fd, pack_package("W", n))
end

-- 处理从节点的握手请求
-- @param fd 从节点的文件描述符
-- @return 从节点的ID和地址
local function handshake(fd)
    -- 读取从节点发送的数据包
    local t, slave_id, slave_addr = read_package(fd)
    -- 确保数据包类型为HANDSHAKE
    assert(t=='H', "Invalid handshake type " .. t)
    -- 确保从节点ID不为0
    assert(slave_id ~= 0 , "Invalid slave id 0")
    -- 如果该从节点ID已存在
    if slave_node[slave_id] then
        -- 抛出错误，提示该从节点已注册
        error(string.format("Slave %d already register on %s", slave_id, slave_node[slave_id].addr))
    end
    -- 向所有从节点报告新从节点的信息，并通知当前从节点等待的从节点数量
    report_slave(fd, slave_id, slave_addr)
    -- 将新从节点的信息添加到slave_node表中
    slave_node[slave_id] = {
        fd = fd,
        id = slave_id,
        addr = slave_addr,
    }
    -- 返回从节点的ID和地址
    return slave_id , slave_addr
end

-- 处理从节点的请求
-- @param fd 从节点的文件描述符
local function dispatch_slave(fd)
    -- 读取从节点发送的数据包
    local t, name, address = read_package(fd)
    -- 如果数据包类型为REGISTER
    if t == 'R' then
        -- 确保地址为数字类型
        assert(type(address)=="number", "Invalid request")
        -- 如果全局名称映射表中不存在该名称
        if not global_name[name] then
            -- 将该名称和地址添加到全局名称映射表中
            global_name[name] = address
        end
        -- 打包一个NAME消息，包含全局名称和地址
        local message = pack_package("N", name, address)
        -- 遍历所有从节点
        for k,v in pairs(slave_node) do
            -- 向每个从节点发送NAME消息
            socket.write(v.fd, message)
        end
    -- 如果数据包类型为QUERY
    elseif t == 'Q' then
        -- 从全局名称映射表中获取该名称对应的地址
        local address = global_name[name]
        -- 如果地址存在
        if address then
            -- 向从节点发送NAME消息，包含全局名称和地址
            socket.write(fd, pack_package("N", name, address))
        end
    -- 其他情况
    else
        -- 记录错误日志，提示无效的从节点消息类型
        skynet.error("Invalid slave message type " .. t)
    end
end

-- 监控从节点的连接状态
-- @param slave_id 从节点的ID
-- @param slave_address 从节点的地址
local function monitor_slave(slave_id, slave_address)
    -- 获取从节点的文件描述符
    local fd = slave_node[slave_id].fd
    -- 记录日志，提示从节点报告信息
    skynet.error(string.format("Harbor %d (fd=%d) report %s", slave_id, fd, slave_address))
    -- 循环处理从节点的请求，直到出现错误
    while pcall(dispatch_slave, fd) do end
    -- 记录日志，提示从节点已断开连接
    skynet.error("slave " ..slave_id .. " is down")
    -- 打包一个DISCONNECT消息，包含从节点的ID
    local message = pack_package("D", slave_id)
    -- 将从节点的文件描述符置为0，表示断开连接
    slave_node[slave_id].fd = 0
    -- 遍历所有从节点
    for k,v in pairs(slave_node) do
        -- 向每个从节点发送DISCONNECT消息
        socket.write(v.fd, message)
    end
    -- 关闭从节点的文件描述符
    socket.close(fd)
end

-- 启动主节点服务
skynet.start(function()
    -- 从环境变量中获取主节点的监听地址
    local master_addr = skynet.getenv "standalone"
    -- 记录日志，提示主节点监听的地址
    skynet.error("master listen socket " .. tostring(master_addr))
    -- 监听指定地址
    local fd = socket.listen(master_addr)
    -- 启动监听套接字
    socket.start(fd , function(id, addr)
        -- 记录日志，提示有新的连接
        skynet.error("connect from " .. addr .. " " .. id)
        -- 启动新的连接
        socket.start(id)
        -- 尝试处理从节点的握手请求
        local ok, slave, slave_addr = pcall(handshake, id)
        -- 如果握手成功
        if ok then
            -- 启动一个新的协程来监控从节点的连接状态
            skynet.fork(monitor_slave, slave, slave_addr)
        -- 握手失败
        else
            -- 记录日志，提示断开连接和错误信息
            skynet.error(string.format("disconnect fd = %d, error = %s", id, slave))
            -- 关闭连接
            socket.close(id)
        end
    end)
end)
