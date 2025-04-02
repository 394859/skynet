-- 该文件的功能是从.launcher服务获取数据并进行格式化输出，最后退出服务。
local skynet = require "skynet"

-- 获取命令行参数
local cmd = { ... }

-- 格式化表格函数，将表格的键值对格式化为字符串
local function format_table(t)
    -- 创建一个空表，用于存储表格的键
    local index = {}
    -- 遍历表格的键，并将其添加到index表中
    for k in pairs(t) do
        table.insert(index, k)
    end
    -- 对index表进行排序
    table.sort(index)
    -- 创建一个空表，用于存储格式化后的键值对
    local result = {}
    -- 遍历排序后的index表，将键值对格式化为字符串并添加到result表中
    for _,v in ipairs(index) do
        table.insert(result, string.format("%s:%s",v,tostring(t[v])))
    end
    -- 将result表中的字符串用制表符连接起来并返回
    return table.concat(result,"\t")
end

-- 打印一行数据的函数，根据数据类型进行不同的处理
local function dump_line(key, value)
    -- 如果值是表格类型
    if type(value) == "table" then
        -- 调用format_table函数格式化表格并打印
        print(key, format_table(value))
    else
        -- 否则直接打印键和值
        print(key,tostring(value))
    end
end

-- 打印列表的函数，对列表中的每个元素调用dump_line函数
local function dump_list(list)
    -- 创建一个空表，用于存储列表的键
    local index = {}
    -- 遍历列表的键，并将其添加到index表中
    for k in pairs(list) do
        table.insert(index, k)
    end
    -- 对index表进行排序
    table.sort(index)
    -- 遍历排序后的index表，对每个元素调用dump_line函数
    for _,v in ipairs(index) do
        dump_line(v, list[v])
    end
end

-- 启动skynet服务
skynet.start(function()
    -- 调用.launcher服务，传入命令行参数，获取返回的列表
    local list = skynet.call(".launcher","lua", table.unpack(cmd))
    -- 如果列表存在
    if list then
        -- 调用dump_list函数打印列表
        dump_list(list)
    end
    -- 退出skynet服务
    skynet.exit()
end)