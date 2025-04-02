---
-- 多播服务模块，用于管理多播频道的创建、删除、订阅、取消订阅和消息发布等操作。
-- 该模块负责维护本地和远程多播频道的状态，并处理跨节点的多播消息传递。
-- @module multicastd
---

-- 引入skynet框架
local skynet = require "skynet"
-- 引入多播核心模块
local mc = require "skynet.multicast.core"
-- 引入数据中心模块
local datacenter = require "skynet.datacenter"

-- 获取当前节点的harbor ID
local harbor_id = skynet.harbor(skynet.self())

-- 存储命令处理函数的表
local command = {}
-- 存储本地频道的表
local channel = {}
-- 存储每个频道的订阅者数量的表
local channel_n = {}
-- 存储远程频道的表
local channel_remote = {}
-- 初始频道ID，与当前节点的harbor ID相同
local channel_id = harbor_id
-- 用于表示无返回值的常量
local NORET = {}

-- 获取节点地址的辅助函数
-- 如果地址不在缓存中，则从数据中心获取
local function get_address(t, id)
    -- 从数据中心获取多播地址
    local v = assert(datacenter.get("multicast", id))
    -- 将地址存入缓存
    t[id] = v
    return v
end

-- 节点地址的元表，使用get_address函数作为索引查找函数
local node_address = setmetatable({}, { __index = get_address })

-- 创建新的本地频道
-- 频道ID的低8位与当前节点的harbor ID相同
function command.NEW()
    -- 循环查找未使用的频道ID
    while channel[channel_id] do
        -- 获取下一个可用的频道ID
        channel_id = mc.nextid(channel_id)
    end
    -- 初始化新频道的订阅者列表
    channel[channel_id] = {}
    -- 初始化新频道的订阅者数量为0
    channel_n[channel_id] = 0
    -- 保存当前频道ID作为返回值
    local ret = channel_id
    -- 获取下一个可用的频道ID
    channel_id = mc.nextid(channel_id)
    return ret
end

-- 删除远程频道
-- 此函数必须由频道的所有者节点调用
function command.DELR(source, c)
    -- 从本地频道表中移除该频道
    channel[c] = nil
    -- 从本地频道订阅者数量表中移除该频道
    channel_n[c] = nil
    return NORET
end

-- 删除频道
-- 如果频道是远程的，将删除命令转发给所有者节点
-- 否则，删除本地频道并通知所有远程节点
function command.DEL(source, c)
    -- 获取频道所属节点的ID
    local node = c % 256
    if node ~= harbor_id then
        -- 如果频道属于其他节点，将删除命令转发给该节点
        skynet.send(node_address[node], "lua", "DEL", c)
        return NORET
    end
    -- 获取该频道的远程订阅者列表
    local remote = channel_remote[c]
    -- 从本地频道表中移除该频道
    channel[c] = nil
    -- 从本地频道订阅者数量表中移除该频道
    channel_n[c] = nil
    -- 从本地远程频道表中移除该频道
    channel_remote[c] = nil
    if remote then
        -- 如果有远程订阅者，通知每个远程节点删除该频道
        for node in pairs(remote) do
            skynet.send(node_address[node], "lua", "DELR", c)
        end
    end
    return NORET
end

-- 向远程节点转发多播消息
-- 频道ID使用会话字段
local function remote_publish(node, channel, source, ...)
    -- 重定向消息到远程节点
    skynet.redirect(node_address[node], source, "multicast", channel, ...)
end

-- 发布消息
-- 对于本地节点，使用消息指针；对于远程节点，解包消息并转换为字符串后发送
local function publish(c , source, pack, size)
    -- 获取该频道的远程订阅者列表
    local remote = channel_remote[c]
    if remote then
        -- 如果有远程订阅者，解包消息并转换为字符串
        local _, msg, sz = mc.unpack(pack, size)
        local msg = skynet.tostring(msg,sz)
        -- 向每个远程节点转发消息
        for node in pairs(remote) do
            remote_publish(node, c, source, msg)
        end
    end

    -- 获取该频道的本地订阅者列表
    local group = channel[c]
    if group == nil or next(group) == nil then
        -- 如果频道没有订阅者，释放消息包
        local pack = mc.bind(pack, 1)
        mc.close(pack)
        return
    end
    -- 将消息包转换为字符串
    local msg = skynet.tostring(pack, size)
    -- 绑定消息包到频道的订阅者数量
    mc.bind(pack, channel_n[c])
    -- 向每个本地订阅者重定向消息
    for k in pairs(group) do
        skynet.redirect(k, source, "multicast", c , msg)
    end
end

-- 注册多播协议
skynet.register_protocol {
    -- 协议名称
    name = "multicast",
    -- 协议ID
    id = skynet.PTYPE_MULTICAST,
    -- 解包函数
    unpack = function(msg, sz)
        return mc.packremote(msg, sz)
    end,
    -- 消息分发函数
    dispatch = function (...) 
        -- 忽略返回值
        skynet.ignoreret()
        -- 调用发布函数处理消息
        publish(...)
    end,
}

-- 发布消息
-- 如果调用者是远程节点，将消息转发给所有者节点；否则，直接发布消息
function command.PUB(source, c, pack, size)
    -- 断言调用者是本地节点
    assert(skynet.harbor(source) == harbor_id)
    -- 获取频道所属节点的ID
    local node = c % 256
    if node ~= harbor_id then
        -- 如果频道属于其他节点，将消息转发给该节点
        remote_publish(node, c, source, mc.remote(pack))
    else
        -- 否则，直接发布消息
        publish(c, source, pack,size)
    end
end

-- 节点订阅频道
-- 此函数必须由频道的所有者节点调用
-- 如果频道不存在，返回true；否则，将该节点添加到远程订阅者列表中
function command.SUBR(source, c)
    -- 获取订阅者节点的ID
    local node = skynet.harbor(source)
    if not channel[c] then
        -- 如果频道不存在，返回true
        return true
    end
    -- 断言订阅者节点不是本地节点，且频道由本地节点创建
    assert(node ~= harbor_id and c % 256 == harbor_id)
    -- 获取该频道的远程订阅者列表
    local group = channel_remote[c]
    if group == nil then
        -- 如果远程订阅者列表为空，初始化一个新的列表
        group = {}
        channel_remote[c] = group
    end
    -- 将订阅者节点添加到远程订阅者列表中
    group[node] = true
end

-- 服务订阅频道
-- 如果频道是远程的，向所有者节点发送SUBR命令进行订阅
function command.SUB(source, c)
    -- 获取频道所属节点的ID
    local node = c % 256
    if node ~= harbor_id then
        -- 如果频道属于其他节点
        if channel[c] == nil then
            -- 如果本地没有该频道的记录
            if skynet.call(node_address[node], "lua", "SUBR", c) then
                -- 向所有者节点发送SUBR命令进行订阅
                return
            end
            if channel[c] == nil then
                -- 双重检查，因为skynet.call可能会让出控制权，期间可能有其他SUB命令执行
                channel[c] = {}
                channel_n[c] = 0
            end
        end
    end
    -- 获取该频道的本地订阅者列表
    local group = channel[c]
    if group and not group[source] then
        -- 如果订阅者不在本地订阅者列表中
        channel_n[c] = channel_n[c] + 1
        group[source] = true
    end
end

-- 节点取消订阅频道
-- 此函数必须由节点调用
function command.USUBR(source, c)
    -- 获取取消订阅者节点的ID
    local node = skynet.harbor(source)
    -- 断言取消订阅者节点不是本地节点
    assert(node ~= harbor_id)
    -- 获取该频道的远程订阅者列表
    local group = assert(channel_remote[c])
    -- 从远程订阅者列表中移除该节点
    group[node] = nil
    return NORET
end

-- 取消订阅频道
-- 如果订阅者为空且频道是远程的，向频道所有者节点发送USUBR命令
function command.USUB(source, c)
    -- 获取该频道的本地订阅者列表
    local group = assert(channel[c])
    if group[source] then
        -- 如果订阅者在本地订阅者列表中
        group[source] = nil
        channel_n[c] = channel_n[c] - 1
        if channel_n[c] == 0 then
            -- 如果订阅者数量为0
            local node = c % 256
            if node ~= harbor_id then
                -- 如果频道属于其他节点
                channel[c] = nil
                channel_n[c] = nil
                -- 向所有者节点发送USUBR命令
                skynet.send(node_address[node], "lua", "USUBR", c)
            end
        end
    end
    return NORET
end

-- 启动skynet服务
skynet.start(function()
    -- 注册lua消息处理函数
    skynet.dispatch("lua", function(_,source, cmd, ...)
        -- 获取命令处理函数
        local f = assert(command[cmd])
        -- 执行命令处理函数
        local result = f(source, ...)
        if result ~= NORET then
            -- 如果有返回值，打包并返回
            skynet.ret(skynet.pack(result))
        end
    end)
    -- 获取当前服务的句柄
    local self = skynet.self()
    -- 获取当前服务的harbor ID
    local id = skynet.harbor(self)
    -- 将当前服务的句柄存入数据中心
    assert(datacenter.set("multicast", id, self) == nil)
end)

