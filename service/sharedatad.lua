-- 该文件是一个基于Skynet框架的共享数据服务，用于管理和维护共享数据对象。
 -- 它提供了创建、查询、更新和删除共享数据对象的功能，并支持对数据对象的监控和垃圾回收。
local skynet = require "skynet"
local sharedata = require "skynet.sharedata.corelib"
local table = table
local cache = require "skynet.codecache"
-- 关闭代码缓存，因为CMD.new可能会加载数据文件
cache.mode "OFF"

-- 定义一个空表，用于表示无返回值
local NORET = {}
-- 定义一个表，用于存储共享数据对象池
local pool = {}
-- 定义一个表，用于记录每个共享数据对象的使用计数和阈值
local pool_count = {}
-- 定义一个表，用于映射共享数据对象和其元数据
local objmap = {}
-- 定义一个计数器，用于控制垃圾回收的时间间隔
local collect_tick = 10

-- 定义一个函数，用于创建新的共享数据对象
local function newobj(name, tbl)
    -- 断言该名称的对象在对象池中不存在
    assert(pool[name] == nil)
    -- 创建一个新的共享数据对象
    local cobj = sharedata.host.new(tbl)
    -- 增加该对象的引用计数
    sharedata.host.incref(cobj)
    -- 定义一个元数据对象，包含对象本身和一个监控列表
    local v = {obj = cobj, watch = {} }
    -- 将对象和元数据进行映射
    objmap[cobj] = v
    -- 将元数据存入对象池
    pool[name] = v
    -- 初始化该对象的使用计数和阈值
    pool_count[name] = { n = 0, threshold = 16 }
end

-- 定义一个函数，用于将垃圾回收时间间隔设置为1分钟
local function collect1min()
    -- 如果计数大于1，则将其设置为1
    if collect_tick > 1 then
        collect_tick = 1
    end
end

-- 定义一个函数，用于定期执行垃圾回收操作
local function collectobj()
    -- 进入一个无限循环
    while true do
        -- 睡眠1分钟
        skynet.sleep(60*100)
        -- 如果计数小于等于0
        if collect_tick <= 0 then
            -- 重置计数为10分钟
            collect_tick = 10
            -- 执行垃圾回收
            collectgarbage()
            -- 遍历对象映射表
            for obj, v in pairs(objmap) do
                -- 如果元数据为true
                if v == true then
                    -- 如果对象的引用计数小于等于0
                    if sharedata.host.getref(obj) <= 0  then
                        -- 从对象映射表中移除该对象
                        objmap[obj] = nil
                        -- 删除该对象
                        sharedata.host.delete(obj)
                    end
                end
            end
        else
            -- 计数减1
            collect_tick = collect_tick - 1
        end
    end
end

-- 定义一个表，用于存储命令处理函数
local CMD = {}

-- 定义一个元表，用于设置环境变量的索引
local env_mt = { __index = _ENV }

-- 定义一个命令处理函数，用于创建新的共享数据对象
function CMD.new(name, t, ...)
    -- 获取参数t的类型
    local dt = type(t)
    -- 定义一个变量，用于存储最终的数据值
    local value
    -- 如果t是表类型
    if dt == "table" then
        -- 直接将t赋值给value
        value = t
    -- 如果t是字符串类型
    elseif dt == "string" then
        -- 创建一个新的表，并设置元表
        value = setmetatable({}, env_mt)
        -- 定义一个函数变量
        local f
        -- 如果t以@开头
        if t:sub(1,1) == "@" then
            -- 加载文件并赋值给f
            f = assert(loadfile(t:sub(2),"bt",value))
        else
            -- 加载字符串并赋值给f
            f = assert(load(t, "=" .. name, "bt",value))
        end
        -- 调用函数并获取返回值
        local _, ret = assert(skynet.pcall(f, ...))
        -- 移除元表
        setmetatable(value, nil)
        -- 如果返回值是表类型
        if type(ret) == "table" then
            -- 将返回值赋值给value
            value = ret
        end
    -- 如果t是nil类型
    elseif dt == "nil" then
        -- 将value设置为空表
        value = {}
    else
        -- 抛出未知数据类型的错误
        error ("Unknown data type " .. dt)
    end
    -- 调用newobj函数创建新的共享数据对象
    newobj(name, value)
end

-- 定义一个命令处理函数，用于删除共享数据对象
function CMD.delete(name)
    -- 从对象池中获取该名称的对象元数据
    local v = assert(pool[name])
    -- 从对象池中移除该对象
    pool[name] = nil
    -- 从使用计数表中移除该对象
    pool_count[name] = nil
    -- 断言该对象在对象映射表中存在
    assert(objmap[v.obj])
    -- 将对象映射表中的该对象标记为待删除
    objmap[v.obj] = true
    -- 减少该对象的引用计数
    sharedata.host.decref(v.obj)
    -- 遍历该对象的监控列表
    for _,response in pairs(v.watch) do
        -- 调用监控回调函数
        response(true)
    end
end

-- 定义一个命令处理函数，用于查询共享数据对象
function CMD.query(name)
    -- 从对象池中获取该名称的对象元数据
    local v = assert(pool[name], name)
    -- 获取对象本身
    local obj = v.obj
    -- 增加该对象的引用计数
    sharedata.host.incref(obj)
    -- 返回该对象
    return v.obj
end

-- 定义一个命令处理函数，用于确认共享数据对象的使用
function CMD.confirm(cobj)
    -- 如果该对象在对象映射表中存在
    if objmap[cobj] then
        -- 减少该对象的引用计数
        sharedata.host.decref(cobj)
    end
    -- 返回无返回值标记
    return NORET
end

-- 定义一个命令处理函数，用于更新共享数据对象
function CMD.update(name, t, ...)
    -- 从对象池中获取该名称的对象元数据
    local v = pool[name]
    -- 定义变量，用于存储监控列表和旧对象
    local watch, oldcobj
    -- 如果对象存在
    if v then
        -- 获取监控列表
        watch = v.watch
        -- 获取旧对象
        oldcobj = v.obj
        -- 将旧对象标记为待删除
        objmap[oldcobj] = true
        -- 减少旧对象的引用计数
        sharedata.host.decref(oldcobj)
        -- 从对象池中移除该对象
        pool[name] = nil
        -- 从使用计数表中移除该对象
        pool_count[name] = nil
    end
    -- 调用CMD.new函数创建新的共享数据对象
    CMD.new(name, t, ...)
    -- 获取新对象
    local newobj = pool[name].obj
    -- 如果监控列表存在
    if watch then
        -- 标记旧对象为脏数据
        sharedata.host.markdirty(oldcobj)
        -- 遍历监控列表
        for _,response in pairs(watch) do
            -- 增加新对象的引用计数
            sharedata.host.incref(newobj)
            -- 调用监控回调函数
            response(true, newobj)
        end
    end
    -- 调用collect1min函数，将垃圾回收时间间隔设置为1分钟
    collect1min()
end

-- 定义一个函数，用于检查监控列表中的无效回调
local function check_watch(queue)
    -- 定义一个计数器，用于记录无效回调的数量
    local n = 0
    -- 遍历监控列表
    for k,response in pairs(queue) do
        -- 如果回调函数返回false
        if not response "TEST" then
            -- 从监控列表中移除该回调
            queue[k] = nil
            -- 计数器加1
            n = n + 1
        end
    end
    -- 返回无效回调的数量
    return n
end

-- 定义一个命令处理函数，用于监控共享数据对象的变化
function CMD.monitor(name, obj)
    -- 从对象池中获取该名称的对象元数据
    local v = assert(pool[name])
    -- 如果传入的对象与当前对象不同
    if obj ~= v.obj then
        -- 增加当前对象的引用计数
        sharedata.host.incref(v.obj)
        -- 返回当前对象
        return v.obj
    end

    -- 增加该对象的使用计数
    local n = pool_count[name].n + 1
    -- 如果使用计数超过阈值
    if n > pool_count[name].threshold then
        -- 检查并移除监控列表中的无效回调
        n = n - check_watch(v.watch)
        -- 将阈值设置为使用计数的两倍
        pool_count[name].threshold = n * 2
    end
    -- 更新使用计数
    pool_count[name].n = n

    -- 将当前响应添加到监控列表中
    table.insert(v.watch, skynet.response())

    -- 返回无返回值标记
    return NORET
end

-- 启动Skynet服务
skynet.start(function()
    -- 启动一个协程，用于执行垃圾回收操作
    skynet.fork(collectobj)
    -- 注册Lua消息处理函数
    skynet.dispatch("lua", function (session, source ,cmd, ...)
        -- 获取对应的命令处理函数
        local f = assert(CMD[cmd])
        -- 调用命令处理函数
        local r = f(...)
        -- 如果返回值不是无返回值标记
        if r ~= NORET then
            -- 将返回值打包并返回
            skynet.ret(skynet.pack(r))
        end
    end)
end)

