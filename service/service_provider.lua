-- 引入skynet库，用于构建服务和消息处理
local skynet = require "skynet"

-- 定义一个空表provider，用于存储服务提供相关的函数
local provider = {}

-- 定义一个内部函数new_service，用于创建新的服务实例
local function new_service(svr, name)
	local s = {}
	svr[name] = s
	s.queue = {}
	return s
end

-- 创建一个元表svr，当访问不存在的键时，调用new_service函数创建新的服务实例
local svr = setmetatable({}, { __index = new_service })


-- 定义provider表中的query函数，用于查询服务地址
function provider.query(name)
	local s = svr[name]
	if s.queue then
		table.insert(s.queue, skynet.response())
	else
		if s.address then
			return skynet.ret(skynet.pack(s.address))
		else
			error(s.error)
		end
	end
end

-- 定义一个内部函数boot，用于启动服务并初始化
local function boot(addr, name, code, ...)
	local s = svr[name]
	skynet.call(addr, "lua", "init", code, ...)
	local tmp = table.pack( ... )
	for i=1,tmp.n do
		tmp[i] = tostring(tmp[i])
	end

	if tmp.n > 0 then
		s.init = table.concat(tmp, ",")
	end
	s.time = skynet.time()
end

-- 定义provider表中的launch函数，用于启动新的服务
function provider.launch(name, code, ...)
	local s = svr[name]
	if s.address then
		return skynet.ret(skynet.pack(s.address))
	end
	if s.booting then
		table.insert(s.queue, skynet.response())
	else
		s.booting = true
		local err
		local ok, addr = pcall(skynet.newservice,"service_cell", name)
		if ok then
			ok, err = xpcall(boot, debug.traceback, addr, name, code, ...)
		else
			err = addr
			addr = nil
		end
		s.booting = nil
		if ok then
			s.address = addr
			for _, resp in ipairs(s.queue) do
				resp(true, addr)
			end
			s.queue = nil
			skynet.ret(skynet.pack(addr))
		else
			if addr then
				skynet.send(addr, "debug", "EXIT")
			end
			s.error = err
			for _, resp in ipairs(s.queue) do
				resp(false)
			end
			s.queue = nil
			error(err)
		end
	end
end

-- 定义provider表中的test函数，用于测试服务状态
function provider.test(name)
	local s = svr[name]
	if s.booting then
		skynet.ret(skynet.pack(nil, true))	-- booting
	elseif s.address then
		skynet.ret(skynet.pack(s.address))
	elseif s.error then
		error(s.error)
	else
		skynet.ret()	-- nil
	end
end

-- 定义provider表中的close函数，用于关闭服务
function provider.close(name)
	local s = svr[name]
	if not s or s.booting then
		return skynet.ret(skynet.pack(nil))
	end

	svr[name] = nil
	skynet.ret(skynet.pack(s.address))
end

-- 启动skynet服务，并设置消息处理和信息获取函数
skynet.start(function()
	skynet.dispatch("lua", function(session, address, cmd, ...)
		provider[cmd](...)
	end)
	skynet.info_func(function()
		local info = {}
		for k,v in pairs(svr) do
			local status
			if v.booting then
				status = "booting"
			elseif v.queue then
				status = "waiting(" .. #v.queue .. ")"
			end
			info[skynet.address(v.address)] = {
				init = v.init,
				name = k,
				time = os.date("%Y %b %d %T %z",math.floor(v.time)),
				status = status,
			}
		end
		return info
	end)
end)
