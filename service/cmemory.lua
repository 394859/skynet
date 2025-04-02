---
-- @file cmemory.lua
-- @brief 该文件主要用于获取和输出Skynet服务的内存使用信息，并在启动后立即退出。
-- 它提供了一种简单的方式来查看服务的内存占用情况，包括每个内存块的大小和总内存使用量。
--
-- @author Your Name
-- @date 2024-07-25
---
local skynet = require "skynet"
local memory = require "skynet.memory"

-- 输出内存使用的详细信息，可能包括每个内存块的大小和其他相关信息
memory.dumpinfo()
-- 注释掉的代码，用于输出完整的内存转储信息，可能用于调试或详细分析
--memory.dump()
-- 获取当前的内存使用信息，返回一个包含内存块信息的表
local info = memory.info()
for k,v in pairs(info) do
	print(string.format("%%08x %%gK",k,v/1024))
end

-- 输出总的内存使用量，以方便查看服务的整体内存占用情况
print("Total memory:", memory.total())
-- 输出总的内存块数量，用于了解内存分配的情况
print("Total block:", memory.block())

-- 启动Skynet服务，并在启动后立即退出，因为该服务主要用于获取内存信息，不需要长时间运行
skynet.start(function() skynet.exit() end)
