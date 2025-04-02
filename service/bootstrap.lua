-- 引入skynet.service模块，用于服务管理
local service = require "skynet.service"
-- 引入skynet.manager模块，该模块包含skynet.launch等函数
local skynet = require "skynet.manager"    -- import skynet.launch, ...

-- 启动skynet服务，传入一个匿名函数作为启动回调
skynet.start(function()
    -- 获取环境变量standalone的值，判断是否为独立模式
    local standalone = skynet.getenv "standalone"

    -- 启动一个名为launcher的snlua服务，并返回其句柄
    local launcher = assert(skynet.launch("snlua","launcher"))
    -- 为该服务命名为.launcher，方便后续引用
    skynet.name(".launcher", launcher)

    -- 获取环境变量harbor的值，并转换为数字类型，默认为0
    local harbor_id = tonumber(skynet.getenv "harbor" or 0)
    -- 如果harbor_id为0，表示是独立模式
    if harbor_id == 0 then
        -- 断言standalone为nil
        assert(standalone ==  nil)
        -- 设置standalone为true
        standalone = true
        -- 设置环境变量standalone为true
        skynet.setenv("standalone", "true")

        -- 尝试启动一个名为cdummy的服务
        local ok, slave = pcall(skynet.newservice, "cdummy")
        -- 如果启动失败，终止skynet服务
        if not ok then
            skynet.abort()
        end
        -- 为该服务命名为.cslave
        skynet.name(".cslave", slave)

    else
        -- 如果是独立模式
        if standalone then
            -- 尝试启动一个名为cmaster的服务
            if not pcall(skynet.newservice,"cmaster") then
                -- 启动失败，终止skynet服务
                skynet.abort()
            end
        end

        -- 尝试启动一个名为cslave的服务
        local ok, slave = pcall(skynet.newservice, "cslave")
        -- 如果启动失败，终止skynet服务
        if not ok then
            skynet.abort()
        end
        -- 为该服务命名为.cslave
        skynet.name(".cslave", slave)
    end

    -- 如果是独立模式
    if standalone then
        -- 启动一个名为datacenterd的服务
        local datacenter = skynet.newservice "datacenterd"
        -- 为该服务命名为DATACENTER
        skynet.name("DATACENTER", datacenter)
    end
    -- 启动一个名为service_mgr的服务
    skynet.newservice "service_mgr"

    -- 获取环境变量enablessl的值
    local enablessl = skynet.getenv "enablessl"
    -- 如果enablessl为true
    if enablessl == "true" then
        -- 创建一个名为ltls_holder的服务，并传入一个匿名函数作为初始化函数
        service.new("ltls_holder", function ()
            -- 引入ltls.init.c模块
            local c = require "ltls.init.c"
            -- 调用该模块的constructor函数
            c.constructor()
        end)
    end

    -- 尝试启动一个由环境变量start指定的服务，默认为main
    pcall(skynet.newservice,skynet.getenv "start" or "main")
    -- 退出skynet服务启动回调
    skynet.exit()
end)
