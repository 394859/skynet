---
-- debug_agent.lua 是一个用于调试代理服务的 Lua 文件。
-- 它的主要功能是处理调试相关的命令，例如启动调试会话、执行调试命令和响应 ping 请求。
-- 该服务通过 skynet 框架实现，使用 debugchannel 模块来处理调试通道。
---
local skynet = require "skynet"
local debugchannel = require "skynet.debugchannel"

local CMD = {}

local channel

-- 定义 CMD 表中的 start 函数，用于启动调试会话
function CMD.start(address, fd)
    -- 断言 channel 变量为空，确保调试会话不会重复启动
    assert(channel == nil, "start more than once")
    -- 使用 skynet.error 函数输出调试信息，显示要连接的地址
    skynet.error(string.format("Attach to :%08x", address))
    -- 定义一个局部变量，用于存储调试通道的句柄
    local handle
    -- 调用 debugchannel.create 函数创建一个新的调试通道，并将返回的通道对象和句柄分别赋值给 channel 和 handle
    channel, handle = debugchannel.create()
    -- 使用 pcall 函数安全地调用 skynet.call 函数，尝试连接到指定地址并发送 REMOTEDEBUG 命令
    local ok, err = pcall(skynet.call, address, "debug", "REMOTEDEBUG", fd, handle)
    -- 如果连接失败
    if not ok then
        -- 使用 skynet.ret 函数返回一个包含错误信息的数据包
        skynet.ret(skynet.pack(false, "Debugger attach failed"))
    else
        -- 注释：此处预留了一个钩子函数的位置，可用于后续扩展功能
        --  todo hook
        -- 如果连接成功，使用 skynet.ret 函数返回一个成功的数据包
        skynet.ret(skynet.pack(true))
    end
    -- 调用 skynet.exit 函数退出当前服务
    skynet.exit()
end

-- 定义 CMD 表中的 cmd 函数，用于执行调试命令
function CMD.cmd(cmdline)
    -- 调用 channel 对象的 write 方法，将调试命令写入调试通道
    channel:write(cmdline)
end

-- 定义 CMD 表中的 ping 函数，用于响应 ping 请求
function CMD.ping()
    -- 使用 skynet.ret 函数返回一个空数据包，表示响应成功
    skynet.ret()
end

-- 调用 skynet.start 函数启动服务
skynet.start(function()
    -- 调用 skynet.dispatch 函数注册一个 Lua 消息处理函数
    skynet.dispatch("lua", function(_,_,cmd,...)
        -- 根据传入的命令字符串从 CMD 表中查找对应的处理函数
        local f = CMD[cmd]
        -- 调用找到的处理函数，并传入剩余的参数
        f(...)
    end)
end)
