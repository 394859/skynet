-- service_cell.lua 文件的主要功能是初始化并管理服务。它接收服务代码并执行，提供了服务启动和命令处理的基础框架。

local skynet = require "skynet"

-- 获取传入的服务名称
local service_name = (...)
-- 初始化一个空表，用于存储初始化相关的函数
local init = {}

-- 定义 init 表中的 init 函数，用于初始化服务
function init.init(code, ...)
    -- 声明一个变量，用于存储服务的启动函数
    local start_func
    -- 重写 skynet.start 函数，将传入的启动函数赋值给 start_func
    skynet.start = function(f)
        start_func = f
    end
    -- 定义一个默认的消息处理函数，当接收到 lua 消息时抛出错误
    skynet.dispatch("lua", function() error("No dispatch function")    end)
    -- 使用 load 函数加载传入的代码块，并将其与服务名称关联
    local mainfunc = assert(load(code, service_name))
    -- 使用 skynet.pcall 安全地调用主函数，并传入额外的参数
    assert(skynet.pcall(mainfunc,...))
    -- 如果存在启动函数，则调用它
    if start_func then
        start_func()
    end
    -- 返回结果
    skynet.ret()
end

-- 启动 skynet 服务
skynet.start(function()
    -- 定义 lua 消息的处理函数
    skynet.dispatch("lua", function(_,_,cmd,...)
        -- 根据传入的命令调用 init 表中对应的函数
        init[cmd](...)
    end)
end)
