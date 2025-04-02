-- 引入skynet模块，提供skynet服务的核心功能
local skynet = require "skynet"
-- 引入skynet.manager模块，包含skynet.launch等函数
require "skynet.manager"    -- import skynet.launch, ...

-- 全局名称表，存储全局名称和对应的地址
local globalname = {}
-- 查询名称表，存储正在查询的名称和对应的响应队列
local queryname = {}
-- 集群相关操作表
local harbor = {}
-- 集群服务句柄
local harbor_service

-- 注册一个名为harbor的协议，用于集群通信
skynet.register_protocol {
    -- 协议名称
    name = "harbor",
    -- 协议ID
    id = skynet.PTYPE_HARBOR,
    -- 打包函数，直接返回参数
    pack = function(...) return ... end,
    -- 解包函数，将数据转换为字符串
    unpack = skynet.tostring,
}

-- 注册一个名为text的协议，用于文本通信
skynet.register_protocol {
    -- 协议名称
    name = "text",
    -- 协议ID
    id = skynet.PTYPE_TEXT,
    -- 打包函数，直接返回参数
    pack = function(...) return ... end,
    -- 解包函数，将数据转换为字符串
    unpack = skynet.tostring,
}

-- 响应名称查询的函数
local function response_name(name)
    -- 获取全局名称表中该名称对应的地址
    local address = globalname[name]
    -- 如果该名称存在查询队列
    if queryname[name] then
        -- 获取查询队列
        local tmp = queryname[name]
        -- 清空查询队列
        queryname[name] = nil
        -- 遍历查询队列中的每个响应函数
        for _,resp in ipairs(tmp) do
            -- 调用响应函数，传入查询结果
            resp(true, address)
        end
    end
end

-- 注册全局名称的函数
function harbor.REGISTER(name, handle)
    -- 断言该名称在全局名称表中不存在
    assert(globalname[name] == nil)
    -- 将该名称和对应的地址存入全局名称表
    globalname[name] = handle
    -- 响应名称查询
    response_name(name)
    -- 将名称注册信息重定向到集群服务
    skynet.redirect(harbor_service, handle, "harbor", 0, "N " .. name)
end

-- 查询全局名称的函数
function harbor.QUERYNAME(name)
    -- 如果名称以.开头，表示是本地名称
    if name:byte() == 46 then    -- "." , local name
        -- 返回本地名称对应的地址
        skynet.ret(skynet.pack(skynet.localname(name)))
        return
    end
    -- 获取全局名称表中该名称对应的地址
    local result = globalname[name]
    -- 如果该名称存在对应的地址
    if result then
        -- 返回该地址
        skynet.ret(skynet.pack(result))
        return
    end
    -- 获取该名称的查询队列
    local queue = queryname[name]
    -- 如果查询队列不存在
    if queue == nil then
        -- 创建一个新的查询队列，并将当前响应函数存入
        queue = { skynet.response() }
        -- 将查询队列存入查询名称表
        queryname[name] = queue
    else
        -- 将当前响应函数存入查询队列
        table.insert(queue, skynet.response())
    end
end

-- 处理链接请求的函数
function harbor.LINK(id)
    -- 返回空响应
    skynet.ret()
end

-- 处理连接请求的函数
function harbor.CONNECT(id)
    -- 输出错误信息，提示在单节点模式下不能连接到其他集群
    skynet.error("Can't connect to other harbor in single node mode")
end

-- 启动skynet服务的函数
skynet.start(function()
    -- 获取环境变量harbor的值，并转换为数字类型
    local harbor_id = tonumber(skynet.getenv "harbor")
    -- 断言harbor_id为0
    assert(harbor_id == 0)

    -- 注册一个lua协议的消息处理函数
    skynet.dispatch("lua", function (session,source,command,...)
        -- 获取命令对应的处理函数
        local f = assert(harbor[command])
        -- 调用处理函数
        f(...)
    end)
    -- 注册一个text协议的消息处理函数
    skynet.dispatch("text", function(session,source,command)
        -- 忽略所有命令
        -- ignore all the command
    end)

    -- 启动一个名为harbor的服务，并传入harbor_id和当前服务的句柄
    harbor_service = assert(skynet.launch("harbor", harbor_id, skynet.self()))
end)
