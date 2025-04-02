-- 引入skynet库
local skynet = require "skynet"
-- 引入skynet.cluster模块
local cluster = require "skynet.cluster"
-- 引入skynet.manager模块，注入skynet.forward_type
require "skynet.manager"	-- inject skynet.forward_type

-- 获取传入的节点和地址
local node, address = ...

-- 注册一个名为system的协议
skynet.register_protocol {
	name = "system",
	id = skynet.PTYPE_SYSTEM,
	unpack = function (...) return ... end,
}

-- 定义协议转发映射表
local forward_map = {
	[skynet.PTYPE_SNAX] = skynet.PTYPE_SYSTEM,
	[skynet.PTYPE_LUA] = skynet.PTYPE_SYSTEM,
	[skynet.PTYPE_RESPONSE] = skynet.PTYPE_RESPONSE,	-- don't free response message
}

-- 设置协议转发类型和处理函数
skynet.forward_type( forward_map ,function()
	-- 获取clusterd服务的唯一实例
	local clusterd = skynet.uniqueservice("clusterd")
	-- 将地址转换为数字类型
	local n = tonumber(address)
	if n then
		address = n
	end
	-- 调用clusterd服务的sender方法获取节点发送器
	local sender = skynet.call(clusterd, "lua", "sender", node)
	-- 注册system协议的消息处理函数
	skynet.dispatch("system", function (session, source, msg, sz)
		if session == 0 then
			-- 发送无返回值的消息
			skynet.send(sender, "lua", "push", address, msg, sz)
		else
			-- 发送有返回值的消息并返回结果
			skynet.ret(skynet.rawcall(sender, "lua", skynet.pack("req", address, msg, sz)))
		end
	end)
end)
