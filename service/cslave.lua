-- 此文件实现了一个Skynet服务，用于管理从节点（slave）的连接和通信。
local skynet = require "skynet"
-- 引入Skynet库
local socket = require "skynet.socket"
-- 引入socket库
local socketdriver = require "skynet.socketdriver"
-- 引入Skynet管理器库
require "skynet.manager" -- import skynet.launch, ...
-- 引入table库
local table = table

-- 存储从节点连接信息的表
local slaves = {}
-- 存储待连接从节点的队列
local connect_queue = {}
-- 存储全局名称与地址映射的表
local globalname = {}
-- 存储查询名称的队列
local queryname = {}
-- 存储从节点相关操作的模块
local harbor = {}
-- 从节点服务的句柄
local harbor_service
-- 存储监控信息的表
local monitor = {}
-- 存储主节点监控信息的表
local monitor_master_set = {}

-- 从socket读取一个数据包
local function read_package(fd)
    -- 读取一个字节表示数据包大小
    local sz = socket.read(fd, 1)
    -- 若读取失败，抛出错误
    assert(sz, "closed")
    -- 将字节转换为数字
    sz = string.byte(sz)
    -- 读取指定大小的数据包内容
    local content = assert(socket.read(fd, sz), "closed")
    -- 解包内容
    return skynet.unpack(content)
end

-- 打包一个数据包
local function pack_package(...) 
    -- 打包参数为字符串
    local message = skynet.packstring(...)
    -- 获取数据包大小
    local size = #message
    -- 确保数据包大小不超过255字节
    assert(size <= 255 , "too long")
    -- 返回带长度前缀的数据包
    return string.char(size) .. message
end

-- 清除指定ID的监控信息
local function monitor_clear(id)
    -- 获取指定ID的监控信息
    local v = monitor[id]
    if v then
        -- 清除监控信息
        monitor[id] = nil
        -- 唤醒所有等待的协程
        for _, v in ipairs(v) do
            v(true)
        end
    end
end

-- 连接到指定的从节点
local function connect_slave(slave_id, address)
    -- 尝试连接从节点
    local ok, err = pcall(function()
        if slaves[slave_id] == nil then
            -- 打开socket连接
            local fd = assert(socket.open(address), "Can't connect to "..address)
            -- 设置socket为无延迟模式
            socketdriver.nodelay(fd)
            -- 记录连接信息
            skynet.error(string.format("Connect to harbor %d (fd=%d), %s", slave_id, fd, address))
            -- 存储从节点连接信息
            slaves[slave_id] = fd
            -- 清除监控信息
            monitor_clear(slave_id)
            -- 放弃对socket的控制
            socket.abandon(fd)
            -- 发送连接信息到从节点服务
            skynet.send(harbor_service, "harbor", string.format("S %d %d",fd,slave_id))
        end
    end)
    if not ok then
        -- 若连接失败，记录错误信息
        skynet.error(err)
    end
end

-- 准备连接所有待连接的从节点
local function ready()
    -- 获取待连接队列
    local queue = connect_queue
    -- 清空待连接队列
    connect_queue = nil
    -- 连接所有待连接的从节点
    for k,v in pairs(queue) do
        connect_slave(k,v)
    end
    -- 重定向所有全局名称到从节点服务
    for name,address in pairs(globalname) do
        skynet.redirect(harbor_service, address, "harbor", 0, "N " .. name)
    end
end

-- 响应名称查询请求
local function response_name(name)
    -- 获取名称对应的地址
    local address = globalname[name]
    if queryname[name] then
        -- 清空查询队列
        local tmp = queryname[name]
        queryname[name] = nil
        -- 响应所有查询请求
        for _,resp in ipairs(tmp) do
            resp(true, address)
        end
    end
end

-- 监控主节点连接
local function monitor_master(master_fd)
    while true do
        -- 尝试从主节点读取数据包
        local ok, t, id_name, address = pcall(read_package,master_fd)
        if ok then
            if t == 'C' then
                -- 处理连接请求
                if connect_queue then
                    connect_queue[id_name] = address
                else
                    connect_slave(id_name, address)
                end
            elseif t == 'N' then
                -- 处理名称注册请求
                globalname[id_name] = address
                response_name(id_name)
                if connect_queue == nil then
                    skynet.redirect(harbor_service, address, "harbor", 0, "N " .. id_name)
                end
            elseif t == 'D' then
                -- 处理从节点断开连接请求
                local fd = slaves[id_name]
                slaves[id_name] = false
                if fd then
                    monitor_clear(id_name)
                    socket.close(fd)
                end
            end
        else
            -- 若读取失败，记录错误信息并关闭连接
            skynet.error("Master disconnect")
            for _, v in ipairs(monitor_master_set) do
                v(true)
            end
            socket.close(master_fd)
            break
        end
    end
end

-- 接受从节点连接
local function accept_slave(fd)
    -- 启动socket连接
    socket.start(fd)
    -- 读取从节点ID
    local id = socket.read(fd, 1)
    if not id then
        -- 若读取失败，记录错误信息并关闭连接
        skynet.error(string.format("Connection (fd =%d) closed", fd))
        socket.close(fd)
        return
    end
    -- 将ID转换为数字
    id = string.byte(id)
    if slaves[id] ~= nil then
        -- 若从节点已存在，记录错误信息并关闭连接
        skynet.error(string.format("Slave %d exist (fd =%d)", id, fd))
        socket.close(fd)
        return
    end
    -- 存储从节点连接信息
    slaves[id] = fd
    -- 清除监控信息
    monitor_clear(id)
    -- 放弃对socket的控制
    socket.abandon(fd)
    -- 记录连接信息
    skynet.error(string.format("Harbor %d connected (fd = %d)", id, fd))
    -- 发送连接信息到从节点服务
    skynet.send(harbor_service, "harbor", string.format("A %d %d", fd, id))
end

-- 注册harbor协议
skynet.register_protocol {
    name = "harbor",
    id = skynet.PTYPE_HARBOR,
    pack = function(...) return ... end,
    unpack = skynet.tostring,
}

-- 注册text协议
skynet.register_protocol {
    name = "text",
    id = skynet.PTYPE_TEXT,
    pack = function(...) return ... end,
    unpack = skynet.tostring,
}

-- 监控从节点连接的处理函数
local function monitor_harbor(master_fd)
    return function(session, source, command)
        -- 获取命令类型
        local t = string.sub(command, 1, 1)
        -- 获取命令参数
        local arg = string.sub(command, 3)
        if t == 'Q' then
            -- 处理名称查询请求
            if globalname[arg] then
                skynet.redirect(harbor_service, globalname[arg], "harbor", 0, "N " .. arg)
            else
                socket.write(master_fd, pack_package("Q", arg))
            end
        elseif t == 'D' then
            -- 处理从节点断开连接请求
            local id = tonumber(arg)
            if slaves[id] then
                monitor_clear(id)
            end
            slaves[id] = false
        else
            -- 处理未知命令
            skynet.error("Unknown command ", command)
        end
    end
end

-- 注册全局名称
function harbor.REGISTER(fd, name, handle)
    -- 确保名称未被注册
    assert(globalname[name] == nil)
    -- 注册名称
    globalname[name] = handle
    -- 响应名称查询请求
    response_name(name)
    -- 发送注册信息到主节点
    socket.write(fd, pack_package("R", name, handle))
    -- 重定向名称到从节点服务
    skynet.redirect(harbor_service, handle, "harbor", 0, "N " .. name)
end

-- 链接到指定从节点
function harbor.LINK(fd, id)
    if slaves[id] then
        if monitor[id] == nil then
            monitor[id] = {}
        end
        -- 将当前协程加入监控队列
        table.insert(monitor[id], skynet.response())
    else
        -- 若从节点不存在，直接返回
        skynet.ret()
    end
end

-- 链接到主节点
function harbor.LINKMASTER()
    -- 将当前协程加入主节点监控队列
    table.insert(monitor_master_set, skynet.response())
end

-- 连接到指定从节点
function harbor.CONNECT(fd, id)
    if not slaves[id] then
        if monitor[id] == nil then
            monitor[id] = {}
        end
        -- 将当前协程加入监控队列
        table.insert(monitor[id], skynet.response())
    else
        -- 若从节点已连接，直接返回
        skynet.ret()
    end
end

-- 查询全局名称
function harbor.QUERYNAME(fd, name)
    if name:byte() == 46 then -- "." , local name
        -- 若为本地名称，直接返回本地名称对应的地址
        skynet.ret(skynet.pack(skynet.localname(name)))
        return
    end
    -- 获取名称对应的地址
    local result = globalname[name]
    if result then
        -- 若名称已注册，直接返回地址
        skynet.ret(skynet.pack(result))
        return
    end
    -- 获取查询队列
    local queue = queryname[name]
    if queue == nil then
        -- 若查询队列为空，发送查询请求到主节点
        socket.write(fd, pack_package("Q", name))
        -- 创建查询队列
        queue = { skynet.response() }
        queryname[name] = queue
    else
        -- 若查询队列不为空，将当前协程加入查询队列
        table.insert(queue, skynet.response())
    end
end

-- 启动服务
skynet.start(function()
    -- 获取主节点地址
    local master_addr = skynet.getenv "master"
    -- 获取从节点ID
    local harbor_id = tonumber(skynet.getenv "harbor")
    -- 获取从节点监听地址
    local slave_address = assert(skynet.getenv "address")
    -- 监听从节点地址
    local slave_fd = socket.listen(slave_address)
    -- 记录连接信息
    skynet.error("slave connect to master " .. tostring(master_addr))
    -- 连接到主节点
    local master_fd = assert(socket.open(master_addr), "Can't connect to master")

    -- 处理lua消息
    skynet.dispatch("lua", function (_,_,command,...)
        -- 获取命令处理函数
        local f = assert(harbor[command])
        -- 执行命令处理函数
        f(master_fd, ...)
    end)
    -- 处理text消息
    skynet.dispatch("text", monitor_harbor(master_fd))

    -- 启动从节点服务
    harbor_service = assert(skynet.launch("harbor", harbor_id, skynet.self()))

    -- 发送握手消息到主节点
    local hs_message = pack_package("H", harbor_id, slave_address)
    socket.write(master_fd, hs_message)
    -- 读取主节点响应
    local t, n = read_package(master_fd)
    -- 确保握手成功
    assert(t == "W" and type(n) == "number", "slave shakehand failed")
    -- 记录等待信息
    skynet.error(string.format("Waiting for %d harbors", n))
    -- 启动主节点监控协程
    skynet.fork(monitor_master, master_fd)
    if n > 0 then
        -- 若需要等待从节点连接
        local co = coroutine.running()
        -- 启动从节点监听协程
        socket.start(slave_fd, function(fd, addr)
            skynet.error(string.format("New connection (fd = %d, %s)",fd, addr))
            socketdriver.nodelay(fd)
            if pcall(accept_slave,fd) then
                -- 统计已连接的从节点数量
                local s = 0
                for k,v in pairs(slaves) do
                    s = s + 1
                end
                if s >= n then
                    -- 若已连接的从节点数量达到要求，唤醒当前协程
                    skynet.wakeup(co)
                end
            end
        end)
        -- 等待从节点连接
        skynet.wait()
    end
    -- 关闭从节点监听socket
    socket.close(slave_fd)
    -- 记录握手完成信息
    skynet.error("Shakehand ready")
    -- 启动连接准备协程
    skynet.fork(ready)
end)
