-- 引入skynet模块，提供skynet服务的核心功能
  local skynet = require "skynet"
-- 引入skynet.core模块，提供底层核心功能
  local core = require "skynet.core"
-- 引入skynet.manager模块，注入管理器API
  require "skynet.manager" -- import manager apis
-- 引入string模块，用于字符串操作
  local string = string
-- 存储所有服务的表
  local services = {}
-- 存储命令处理函数的表
  local command = {}
-- 用于确认服务启动、错误和启动成功的表
  local instance = {} -- for confirm (function command.LAUNCH / command.ERROR / command.LAUNCHOK)
-- 用于查询服务地址的会话表
  local launch_session = {} -- for command.QUERY, service_address -> session
-- 将服务句柄转换为地址的函数
  local function handle_to_address(handle)
    -- 将十六进制字符串转换为数字地址
    return tonumber("0x" .. string.sub(handle , 2))
  end
-- 表示无返回值的常量
  local NORET = {}
-- 列出所有服务的命令处理函数
  function command.LIST()
    -- 存储服务列表的表
    local list = {}
    -- 遍历所有服务
    for k,v in pairs(services) do
      -- 将服务地址和服务信息存入列表
      list[skynet.address(k)] = v
    end
    -- 返回服务列表
    return list
  end
-- 列出服务状态的辅助函数
  local function list_srv(ti, fmt_func, ...)
    -- 存储服务状态列表的表
    local list = {}
    -- 存储会话的表
    local sessions = {}
    -- 创建一个请求对象
    local req = skynet.request()
    -- 遍历所有服务
    for addr in pairs(services) do
      -- 构造请求参数
      local r = { addr, "debug", ... }
      -- 将请求添加到请求对象中
      req:add(r)
      -- 将会话与服务地址关联
      sessions[r] = addr
    end
    -- 选择请求并处理响应
    for req, resp in req:select(ti) do
      -- 获取服务地址
      local addr = req[1]
      -- 如果有响应
      if resp then
        -- 获取服务状态
        local stat = resp[1]
        -- 将服务状态存入列表
        list[skynet.address(addr)] = fmt_func(stat, addr)
      else
        -- 如果没有响应，将错误信息存入列表
        list[skynet.address(addr)] = fmt_func("ERROR", addr)
      end
      -- 将会话从会话表中移除
      sessions[req] = nil
    end
    -- 处理超时的会话
    for session, addr in pairs(sessions) do
      -- 将超时信息存入列表
      list[skynet.address(addr)] = fmt_func("TIMEOUT", addr)
    end
    -- 返回服务状态列表
    return list
  end
-- 获取服务状态的命令处理函数
  function command.STAT(addr, ti)
    -- 调用list_srv函数获取服务状态
    return list_srv(ti, function(v) return v end, "STAT")
  end
-- 杀死服务的命令处理函数
  function command.KILL(_, handle)
    -- 杀死指定服务
    skynet.kill(handle)
    -- 存储服务信息的返回表
    local ret = { [skynet.address(handle)] = tostring(services[handle]) }
    -- 从服务表中移除该服务
    services[handle] = nil
    -- 返回服务信息
    return ret
  end
-- 获取服务内存使用情况的命令处理函数
  function command.MEM(addr, ti)
    -- 调用list_srv函数获取服务内存使用情况
    return list_srv(ti, function(kb, addr)
      -- 获取服务信息
      local v = services[addr]
      -- 如果内存使用情况是字符串类型
      if type(kb) == "string" then
        -- 返回格式化后的字符串
        return string.format("%s (%s)", kb, v)
      else
        -- 返回格式化后的字符串
        return string.format("%.2f Kb (%s)",kb,v)
      end
    end, "MEM")
  end
-- 触发服务垃圾回收的命令处理函数
  function command.GC(addr, ti)
    -- 遍历所有服务
    for k,v in pairs(services) do
      -- 向服务发送GC命令
      skynet.send(k,"debug","GC")
    end
    -- 调用MEM命令获取服务内存使用情况
    return command.MEM(addr, ti)
  end
-- 移除服务的命令处理函数
  function command.REMOVE(_, handle, kill)
    -- 从服务表中移除该服务
    services[handle] = nil
    -- 获取服务启动时的响应对象
    local response = instance[handle]
    -- 如果有响应对象
    if response then
      -- 表示服务已死亡
      -- instance is dead
      -- 通知调用者服务已移除，当kill为false时返回nil
      response(not kill) -- return nil to caller of newservice, when kill == false
      -- 从实例表中移除该服务
      instance[handle] = nil
      -- 从启动会话表中移除该服务
      launch_session[handle] = nil
    end
    -- 不返回值，因为服务可能已经退出
    -- don't return (skynet.ret) because the handle may exit
    return NORET
  end
-- 启动服务的辅助函数
  local function launch_service(service, ...)
    -- 将参数拼接成字符串
    local param = table.concat({...}, " ")
    -- 启动服务
    local inst = skynet.launch(service, param)
    -- 获取当前会话
    local session = skynet.context()
    -- 获取响应对象
    local response = skynet.response()
    -- 如果服务启动成功
    if inst then
      -- 将服务信息存入服务表
      services[inst] = service .. " " .. param
      -- 将响应对象存入实例表
      instance[inst] = response
      -- 将会话存入启动会话表
      launch_session[inst] = session
    else
      -- 如果服务启动失败，通知调用者
      response(false)
      -- 返回
      return
    end
    -- 返回服务实例
    return inst
  end
-- 启动服务的命令处理函数
  function command.LAUNCH(_, service, ...)
    -- 调用launch_service函数启动服务
    launch_service(service, ...)
    -- 返回无返回值常量
    return NORET
  end
-- 启动服务并记录日志的命令处理函数
  function command.LOGLAUNCH(_, service, ...)
    -- 调用launch_service函数启动服务
    local inst = launch_service(service, ...)
    -- 如果服务启动成功
    if inst then
      -- 开启服务日志
      core.command("LOGON", skynet.address(inst))
    end
    -- 返回无返回值常量
    return NORET
  end
-- 处理服务启动错误的命令处理函数
  function command.ERROR(address)
    -- 参考serivce-src/service_lua.c
    -- see serivce-src/service_lua.c
    -- 表示服务初始化失败
    -- init failed
    -- 获取服务启动时的响应对象
    local response = instance[address]
    -- 如果有响应对象
    if response then
      -- 通知调用者服务启动失败
      response(false)
      -- 从启动会话表中移除该服务
      launch_session[address] = nil
      -- 从实例表中移除该服务
      instance[address] = nil
    end
    -- 从服务表中移除该服务
    services[address] = nil
    -- 返回无返回值常量
    return NORET
  end
-- 处理服务启动成功通知的命令处理函数
  function command.LAUNCHOK(address)
    -- 表示服务初始化通知
    -- init notice
    -- 获取服务启动时的响应对象
    local response = instance[address]
    if response then
      response(true, address)
      instance[address] = nil
      launch_session[address] = nil
    end

    return NORET
  end

  function command.QUERY(_, request_session)
    for address, session in pairs(launch_session) do
      if session == request_session then
        return address
      end
    end
  end

  -- for historical reasons, launcher support text command (for C service)

  skynet.register_protocol {
    name = "text",
    id = skynet.PTYPE_TEXT,
    unpack = skynet.tostring,
    dispatch = function(session, address , cmd)
      if cmd == "" then
        command.LAUNCHOK(address)
      elseif cmd == "ERROR" then
        command.ERROR(address)
      else
        error ("Invalid text command " .. cmd)
      end
    end,
  }

  skynet.dispatch("lua", function(session, address, cmd , ...)
    cmd = string.upper(cmd)
    local f = command[cmd]
    if f then
      local ret = f(address, ...)
      if ret ~= NORET then
        skynet.ret(skynet.pack(ret))
      end
    else
      skynet.ret(skynet.pack {"Unknown command"} )
    end
  end)

  skynet.start(function() end)
