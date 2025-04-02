local skynet = require "skynet"
local snax   = require "skynet.snax"
local socket = require "skynet.socket"

---
-- @file console.lua
-- @brief 该文件实现了一个控制台服务，用于接收用户输入的命令并执行相应的操作。
-- 支持创建新的Skynet服务和Snax服务。
--
-- @author Your Name
-- @date 2024-07-25
---
local function split_cmdline(cmdline)
	local split = {}
	for i in string.gmatch(cmdline, "%S+") do
		table.insert(split,i)
	end
	return split
end

local function console_main_loop()
	local stdin = socket.stdin()
	while true do
		local cmdline = socket.readline(stdin, "\n")
		local split = split_cmdline(cmdline)
		local command = split[1]
		if command == "snax" then
			pcall(snax.newservice, select(2, table.unpack(split)))
		elseif cmdline ~= "" then
			pcall(skynet.newservice, cmdline)
		end
	end
end

skynet.start(function()
	skynet.fork(console_main_loop)
end)
