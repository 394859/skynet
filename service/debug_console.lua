-- 文件功能概述：
-- debug_console.lua是Skynet框架的调试控制台服务，提供了对Skynet服务的监控、管理和调试功能。
-- 主要功能包括：服务启动、服务状态查询、内存监控、垃圾回收、代码注入等。
-- 该服务通过TCP socket连接，接受命令行指令并执行相应操作。

-- 引入Skynet核心模块
local skynet = require "skynet"  
-- 引入代码缓存模块
local codecache = require "skynet.codecache"  
-- 引入Skynet核心功能模块
local core = require "skynet.core"  
-- 引入socket模块
local socket = require "skynet.socket"
-- 引入snax模块
local snax = require "skynet.snax"
-- 引入内存管理模块
local memory = require "skynet.memory"
-- 引入HTTP处理模块
local httpd = require "http.httpd"
-- 引入HTTP辅助模块
local sockethelper = require "http.sockethelper"

-- 将传入的参数打包成表
local arg = table.pack(...)
-- 断言传入的参数数量不超过2个
assert(arg.n <= 2)
-- 如果传入2个参数，取第一个作为IP地址，否则默认使用127.0.0.1
local ip = (arg.n == 2 and arg[1] or "127.0.0.1")
-- 将最后一个参数转换为数字作为端口号
local port = tonumber(arg[arg.n])
-- 设置超时时间为3秒
local TIMEOUT = 300 -- 3 sec

-- 定义一个空表，用于存储普通命令处理函数
local COMMAND = {}
-- 定义一个空表，用于存储特殊命令处理函数
local COMMANDX = {}

-- 格式化表格数据，将表格的键值对按键排序并格式化为字符串
local function format_table(t)
    -- 用于存储表格的键
    local index = {}
    -- 遍历表格，将键添加到index表中
    for k in pairs(t) do
        table.insert(index, k)
    end
    -- 对键进行排序
    table.sort(index, function(a, b) return tostring(a) < tostring(b) end)
    -- 用于存储格式化后的结果
    local result = {}
    -- 遍历排序后的键，将键值对格式化为字符串并添加到result表中
    for _,v in ipairs(index) do
        table.insert(result, string.format("%s:%s",v,tostring(t[v])))
    end
    -- 将result表中的元素用制表符连接成字符串并返回
    return table.concat(result,"\t")
end

-- 打印一行数据，如果值是表格，则调用format_table进行格式化输出
local function dump_line(print, key, value)
    if type(value) == "table" then
        print(key, format_table(value))
    else
        print(key,tostring(value))
    end
end

-- 打印列表数据，将列表中的元素按键排序并逐行输出
local function dump_list(print, list)
    -- 用于存储列表的键
    local index = {}
    -- 遍历列表，将键添加到index表中
    for k in pairs(list) do
        table.insert(index, k)
    end
    -- 对键进行排序
    table.sort(index, function(a, b) return tostring(a) < tostring(b) end)
    -- 遍历排序后的键，调用dump_line打印键值对
    for _,v in ipairs(index) do
        dump_line(print, v, list[v])
    end
end

-- 将命令行字符串按空格分割成参数列表
local function split_cmdline(cmdline)
    -- 用于存储分割后的参数
    local split = {}
    -- 使用正则表达式匹配非空字符串，将匹配结果添加到split表中
    for i in string.gmatch(cmdline, "%S+") do
        table.insert(split,i)
    end
    -- 返回分割后的参数列表
    return split
end

-- 执行命令行指令，根据指令调用相应的命令处理函数
local function docmd(cmdline, print, fd)
    -- 分割命令行字符串
    local split = split_cmdline(cmdline)
    -- 获取命令名
    local command = split[1]
    -- 从COMMAND表中查找命令处理函数
    local cmd = COMMAND[command]
    local ok, list
    if cmd then
        -- 调用命令处理函数，并捕获可能的错误
        ok, list = pcall(cmd, table.unpack(split,2))
    else
        -- 如果COMMAND表中没有找到，从COMMANDX表中查找
        cmd = COMMANDX[command]
        if cmd then
            -- 将文件描述符添加到参数表中
            split.fd = fd
            -- 将原始命令行字符串赋值给参数表的第一个元素
            split[1] = cmdline
            -- 调用命令处理函数，并捕获可能的错误
            ok, list = pcall(cmd, split)
        else
            -- 如果都没有找到，提示无效命令
            print("Invalid command, type help for command list")
        end
    end

    if ok then
        if list then
            if type(list) == "string" then
                -- 如果返回值是字符串，直接打印
                print(list)
            else
                -- 如果返回值是列表，调用dump_list打印
                dump_list(print, list)
            end
        end
        -- 打印命令执行成功标志
        print("<CMD OK>")
    else
        -- 打印错误信息
        print(list)
        -- 打印命令执行失败标志
        print("<CMD Error>")
    end
end

-- 控制台主循环，处理用户输入的命令
local function console_main_loop(stdin, print, addr)
    -- 打印欢迎信息
    print("Welcome to skynet console")
    -- 记录客户端连接信息
    skynet.error(addr, "connected")
    local ok, err = pcall(function()
        while true do
            -- 从标准输入读取一行命令
            local cmdline = socket.readline(stdin, "\n")
            if not cmdline then
                -- 如果没有读取到命令，退出循环
                break
            end
            if cmdline:sub(1,4) == "GET " then
                -- 如果是HTTP GET请求
                local code, url = httpd.read_request(sockethelper.readfunc(stdin, cmdline.. "\n"), 8192)
                local cmdline = url:sub(2):gsub("/"," ")
                -- 执行命令
                docmd(cmdline, print, stdin)
                break
            elseif cmdline:sub(1,5) == "POST " then
                -- 如果是HTTP POST请求
                local code, url, method, header, body = httpd.read_request(sockethelper.readfunc(stdin, cmdline.. "\n"), 8192)
                -- 执行命令
                docmd(body, print, stdin)
                break
            end
            
            if cmdline ~= "" then
                -- 如果命令不为空，执行命令
                docmd(cmdline, print, stdin)
            end
        end
    end)
    if not ok then
        -- 记录错误信息
        skynet.error(stdin, err)
    end
    -- 记录客户端断开连接信息
    skynet.error(addr, "disconnect")
    -- 关闭标准输入
    socket.close(stdin)
end

-- 启动Skynet服务
skynet.start(function()
    -- 监听指定的IP地址和端口号
    local listen_socket, ip, port = socket.listen (ip, port)
    -- 记录调试控制台启动信息
    skynet.error("Start debug console at " .. ip .. ":" .. port)
    -- 启动监听套接字
    socket.start(listen_socket , function(id, addr)
        -- 定义打印函数，将内容发送到客户端
        local function print(...)
            local t = { ... }
            for k,v in ipairs(t) do
                t[k] = tostring(v)
            end
            socket.write(id, table.concat(t,"\t"))
            socket.write(id, "\n")
        end
        -- 启动客户端套接字
        socket.start(id)
        -- 启动一个新的协程处理客户端连接
        skynet.fork(console_main_loop, id , print, addr)
    end)
end)

-- 帮助命令处理函数，返回所有命令的帮助信息
function COMMAND.help()
    return {
        help = "This help message",
        list = "List all the service",
        stat = "Dump all stats",
        info = "info address : get service infomation",
        exit = "exit address : kill a lua service",
        kill = "kill address : kill service",
        mem = "mem : show memory status",
        gc = "gc : force every lua service do garbage collect",
        start = "lanuch a new lua service",
        snax = "lanuch a new snax service",
        clearcache = "clear lua code cache",
        service = "List unique service",
        task = "task address : show service task detail",
        uniqtask = "task address : show service unique task detail",
        inject = "inject address luascript.lua",
        logon = "logon address",
        logoff = "logoff address",
        log = "launch a new lua service with log",
        debug = "debug address : debug a lua service",
        signal = "signal address sig",
        cmem = "Show C memory info",
        jmem = "Show jemalloc mem stats",
        ping = "ping address",
        call = "call address ...",
        trace = "trace address [proto] [on|off]",
        netstat = "netstat : show netstat",
        profactive = "profactive [on|off] : active/deactive jemalloc heap profilling",
        dumpheap = "dumpheap : dump heap profilling",
        killtask = "killtask address threadname : threadname listed by task",
        dbgcmd = "run address debug command",
        getenv = "getenv name : skynet.getenv(name)",
        setenv = "setenv name value: skynet.setenv(name,value)",
    }
end

-- 清除代码缓存命令处理函数
function COMMAND.clearcache()
    codecache.clear()
end

-- 启动新的Lua服务命令处理函数
function COMMAND.start(...)
    -- 尝试启动新的Lua服务
    local ok, addr = pcall(skynet.newservice, ...)
    if ok then
        if addr then
            -- 如果启动成功，返回服务地址和参数
            return { [skynet.address(addr)] = ... }
        else
            -- 如果启动失败，返回Exit
            return "Exit"
        end
    else
        -- 如果调用出错，返回Failed
        return "Failed"
    end
end

-- 启动带日志的新Lua服务命令处理函数
function COMMAND.log(...)
    -- 尝试启动带日志的新Lua服务
    local ok, addr = pcall(skynet.call, ".launcher", "lua", "LOGLAUNCH", "snlua", ...)
    if ok then
        if addr then
            -- 如果启动成功，返回服务地址和参数
            return { [skynet.address(addr)] = ... }
        else
            -- 如果启动失败，返回Failed
            return "Failed"
        end
    else
        -- 如果调用出错，返回Failed
        return "Failed"
    end
end

-- 启动新的snax服务命令处理函数
function COMMAND.snax(...)
    -- 尝试启动新的snax服务
    local ok, s = pcall(snax.newservice, ...)
    if ok then
        -- 获取服务句柄
        local addr = s.handle
        -- 返回服务地址和参数
        return { [skynet.address(addr)] = ... }
    else
        -- 如果调用出错，返回Failed
        return "Failed"
    end
end

-- 列出唯一服务命令处理函数
function COMMAND.service()
    -- 调用SERVICE服务的LIST方法
    return skynet.call("SERVICE", "lua", "LIST")
end

-- 调整服务地址格式
local function adjust_address(address)
    -- 获取地址的第一个字符
    local prefix = address:sub(1,1)
    if prefix == '.' then
        -- 如果以.开头，通过本地名称查找地址
        return assert(skynet.localname(address), "Not a valid name")
    elseif prefix ~= ':' then
        -- 如果不以:开头，将地址转换为十六进制并与当前节点的Harbor ID组合
        address = assert(tonumber("0x" .. address), "Need an address") | (skynet.harbor(skynet.self()) << 24)
    end
    -- 返回调整后的地址
    return address
end

-- 列出所有服务命令处理函数
function COMMAND.list()
    -- 调用.launcher服务的LIST方法
    return skynet.call(".launcher", "lua", "LIST")
end

-- 处理超时时间参数，将其转换为有效的超时时间
local function timeout(ti)
    if ti then
        -- 将参数转换为数字
        ti = tonumber(ti)
        if ti <= 0 then
            -- 如果超时时间小于等于0，将其置为nil
            ti = nil
        end
    else
        -- 如果没有提供超时时间，使用默认值
        ti = TIMEOUT
    end
    -- 返回超时时间
    return ti
end

-- 转储所有统计信息命令处理函数
function COMMAND.stat(ti)
    -- 调用.launcher服务的STAT方法，并传入超时时间
    return skynet.call(".launcher", "lua", "STAT", timeout(ti))
end

-- 显示内存状态命令处理函数
function COMMAND.mem(ti)
    -- 调用.launcher服务的MEM方法，并传入超时时间
    return skynet.call(".launcher", "lua", "MEM", timeout(ti))
end

-- 杀死服务命令处理函数
function COMMAND.kill(address)
    -- 调用.launcher服务的KILL方法，并传入调整后的服务地址
    return skynet.call(".launcher", "lua", "KILL", adjust_address(address))
end

-- 强制所有Lua服务进行垃圾回收命令处理函数
function COMMAND.gc(ti)
    -- 调用.launcher服务的GC方法，并传入超时时间
    return skynet.call(".launcher", "lua", "GC", timeout(ti))
end

-- 退出服务命令处理函数
function COMMAND.exit(address)
    -- 向指定服务发送EXIT调试命令
    skynet.send(adjust_address(address), "debug", "EXIT")
end

-- 注入代码命令处理函数
function COMMAND.inject(address, filename, ...)
    -- 调整服务地址
    address = adjust_address(address)
    -- 以二进制只读模式打开文件
    local f = io.open(filename, "rb")
    if not f then
        -- 如果文件打开失败，返回错误信息
        return "Can't open " .. filename
    end
    -- 读取文件内容
    local source = f:read "*a"
    -- 关闭文件
    f:close()
    -- 调用指定服务的RUN调试命令，执行注入的代码
    local ok, output = skynet.call(address, "debug", "RUN", source, filename, ...)
    if ok == false then
        -- 如果调用失败，抛出错误
        error(output)
    end
    -- 返回执行结果
    return output
end

-- 运行调试命令处理函数
function COMMAND.dbgcmd(address, cmd, ...)
    -- 调整服务地址
    address = adjust_address(address)
    -- 调用指定服务的调试命令
    return skynet.call(address, "debug", cmd, ...)
end

-- 显示服务任务详情命令处理函数
function COMMAND.task(address)
    -- 调用指定服务的TASK调试命令
    return COMMAND.dbgcmd(address, "TASK")
end

-- 杀死服务任务命令处理函数
function COMMAND.killtask(address, threadname)
    -- 调用指定服务的KILLTASK调试命令
    return COMMAND.dbgcmd(address, "KILLTASK", threadname)
end

-- 显示服务唯一任务详情命令处理函数
function COMMAND.uniqtask(address)
    -- 调用指定服务的UNIQTASK调试命令
    return COMMAND.dbgcmd(address, "UNIQTASK")
end

-- 获取服务信息命令处理函数
function COMMAND.info(address, ...)
    -- 调用指定服务的INFO调试命令
    return COMMAND.dbgcmd(address, "INFO", ...)
end

-- 调试服务命令处理函数
function COMMANDX.debug(cmd)
    -- 调整服务地址
    local address = adjust_address(cmd[2])
    -- 创建一个新的调试代理服务
    local agent = skynet.newservice "debug_agent"
    local stop
    -- 获取当前协程
    local term_co = coroutine.running()
    -- 定义一个函数，用于转发调试命令
    local function forward_cmd()
        repeat
            -- 检测调试代理服务是否存活
            skynet.call(agent, "lua", "ping")    -- detect agent alive, if agent exit, raise error
            -- 从文件描述符读取一行命令
            local cmdline = socket.readline(cmd.fd, "\n")
            cmdline = cmdline and cmdline:gsub("(.*)\r$", "%1")
            if not cmdline then
                -- 如果没有读取到命令，向调试代理服务发送cont命令
                skynet.send(agent, "lua", "cmd", "cont")
                break
            end
            -- 向调试代理服务发送命令
            skynet.send(agent, "lua", "cmd", cmdline)
        until stop or cmdline == "cont"
    end
    -- 启动一个新的协程转发调试命令
    skynet.fork(function()
        pcall(forward_cmd)
        if not stop then    -- block at skynet.call "start"
            term_co = nil
        else
            -- 唤醒当前协程
            skynet.wakeup(term_co)
        end
    end)
    -- 调用调试代理服务的start方法，开始调试
    local ok, err = skynet.call(agent, "lua", "start", address, cmd.fd)
    stop = true
    if term_co then
        -- 等待转发协程退出
        skynet.wait(term_co)
    end

    if not ok then
        -- 如果调试失败，抛出错误
        error(err)
    end
end

-- 开启服务日志命令处理函数
function COMMAND.logon(address)
    -- 调整服务地址
    address = adjust_address(address)
    -- 调用核心命令开启服务日志
    core.command("LOGON", skynet.address(address))
end

-- 关闭服务日志命令处理函数
function COMMAND.logoff(address)
    -- 调整服务地址
    address = adjust_address(address)
    -- 调用核心命令关闭服务日志
    core.command("LOGOFF", skynet.address(address))
end

-- 向服务发送信号命令处理函数
function COMMAND.signal(address, sig)
    -- 调整服务地址并转换为字符串
    address = skynet.address(adjust_address(address))
    if sig then
        -- 如果提供了信号，发送带信号的命令
        core.command("SIGNAL", string.format("%s %d",address,sig))
    else
        -- 如果没有提供信号，发送不带信号的命令
        core.command("SIGNAL", address)
    end
end

-- 显示C内存信息命令处理函数
function COMMAND.cmem()
    -- 获取C内存信息
    local info = memory.info()
    -- 用于存储格式化后的内存信息
    local tmp = {}
    for k,v in pairs(info) do
        -- 将内存信息的键转换为服务地址字符串
        tmp[skynet.address(k)] = v
    end
    -- 记录总内存信息
    tmp.total = memory.total()
    -- 记录块内存信息
    tmp.block = memory.block()

    -- 返回格式化后的内存信息
    return tmp
end

-- 显示jemalloc内存统计信息命令处理函数
function COMMAND.jmem()
    -- 获取jemalloc内存统计信息
    local info = memory.jestat()
    -- 用于存储格式化后的内存统计信息
    local tmp = {}
    for k,v in pairs(info) do
        -- 将内存统计信息格式化为字符串
        tmp[k] = string.format("%11d  %8.2f Mb", v, v/1048576)
    end
    -- 返回格式化后的内存统计信息
    return tmp
end

-- 向服务发送PING命令并返回响应时间命令处理函数
function COMMAND.ping(address)
    -- 调整服务地址
    address = adjust_address(address)
    -- 记录当前时间
    local ti = skynet.now()
    -- 向服务发送PING调试命令
    skynet.call(address, "debug", "PING")
    -- 计算响应时间
    ti = skynet.now() - ti
    -- 返回响应时间的字符串表示
    return tostring(ti)
end

-- 将字符串转换为布尔值
local function toboolean(x)
    return x and (x == "true" or x == "on")
end

-- 开启或关闭服务跟踪日志命令处理函数
function COMMAND.trace(address, proto, flag)
    -- 调整服务地址
    address = adjust_address(address)
    if flag == nil then
        if proto == "on" or proto == "off" then
            -- 如果只提供了一个参数，将其转换为布尔值
            proto = toboolean(proto)
        end
    else
        -- 将标志参数转换为布尔值
        flag = toboolean(flag)
    end
    -- 调用服务的TRACELOG调试命令，开启或关闭跟踪日志
    skynet.call(address, "debug", "TRACELOG", proto, flag)
end

-- 调用服务方法命令处理函数
function COMMANDX.call(cmd)
    -- 调整服务地址
    local address = adjust_address(cmd[2])
    -- 提取命令行参数
    local cmdline = assert(cmd[1]:match("%S+%s+%S+%s(.+)"), "need arguments")
    -- 加载参数并返回函数
    local args_func = assert(load("return " .. cmdline, "debug console", "t", {}), "Invalid arguments")
    -- 调用参数函数并捕获结果
    local args = table.pack(pcall(args_func))
    if not args[1] then
        -- 如果调用失败，抛出错误
        error(args[2])
    end
    -- 调用服务的lua方法并捕获结果
    local rets = table.pack(skynet.call(address, "lua", table.unpack(args, 2, args.n)))
    -- 返回调用结果
    return rets
end

-- 将字节数转换为易读的格式
local function bytes(size)
    if size == nil or size == 0 then
        return
    end
    if size < 1024 then
        return size
    end
    if size < 1024 * 1024 then
        return tostring(size/1024) .. "K"
    end
    return tostring(size/(1024*1024)) .. "M"
end

-- 转换统计信息的格式
local function convert_stat(info)
    -- 获取当前时间
    local now = skynet.now()
    -- 定义一个函数，将时间戳转换为易读的格式
    local function time(t)
        if t == nil then
            return
        end
        t = now - t
        if t < 6000 then
            return tostring(t/100) .. "s"
        end
        local hour = t // (100*60*60)
        t = t - hour * 100 * 60 * 60
        local min = t // (100*60)
        t = t - min * 100 * 60
        local sec = t / 100
        return string.format("%s%d:%.2gs",hour == 0 and "" or (hour .. ":"),min,sec)
    end

    -- 将服务地址转换为字符串
    info.address = skynet.address(info.address)
    -- 将读取字节数转换为易读的格式
    info.read = bytes(info.read)
    -- 将写入字节数转换为易读的格式
    info.write = bytes(info.write)
    -- 将写缓冲区大小转换为易读的格式
    info.wbuffer = bytes(info.wbuffer)
    -- 将读取时间转换为易读的格式
    info.rtime = time(info.rtime)
    -- 将写入时间转换为易读的格式
    info.wtime = time(info.wtime)
end

-- 显示网络统计信息命令处理函数
function COMMAND.netstat()
    -- 获取网络统计信息
    local stat = socket.netstat()
    for _, info in ipairs(stat) do
        -- 转换统计信息的格式
        convert_stat(info)
    end
    -- 返回转换后的网络统计信息
    return stat
end

-- 转储堆内存分析信息命令处理函数
function COMMAND.dumpheap()
    -- 调用内存模块的dumpheap方法
    memory.dumpheap()
end

-- 激活或停用jemalloc堆内存分析命令处理函数
function COMMAND.profactive(flag)
    if flag ~= nil then
        if flag == "on" or flag == "off" then
            -- 将标志参数转换为布尔值
            flag = toboolean(flag)
        end
        -- 激活或停用jemalloc堆内存分析
        memory.profactive(flag)
    end
    -- 获取当前堆内存分析状态
    local active = memory.profactive()
    -- 返回堆内存分析状态的字符串表示
    return "heap profilling is ".. (active and "active" or "deactive")
end

-- 获取Skynet环境变量命令处理函数
function COMMAND.getenv(name)
    -- 获取指定名称的环境变量值
    local value = skynet.getenv(name)
    -- 返回环境变量的键值对
    return {[name]=tostring(value)}
end

-- 设置Skynet环境变量命令处理函数
function COMMAND.setenv(name,value)
    -- 设置指定名称的环境变量值
    return skynet.setenv(name,value)
end
