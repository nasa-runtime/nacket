-- 实时读取也保留具体游标与累计已读数量；裁剪缺口需要状态重建，不能悄悄跳到新末尾。
local stream, action = KEYS[1], ARGV[1]
local typ = redis.call('TYPE', stream).ok
if typ == 'none' then
    if action == 'initialize' then
        return {'0-0', 0}
    end
    if ARGV[2] == '0-0' and ARGV[3] == '0' then
        if action == 'observe' then
            return 0
        end
        return {}
    end
    return redis.error_reply('SC_REALTIME_GAP')
end
if typ ~= 'stream' then
    return redis.error_reply('SC_REALTIME_RESOURCE_TYPE')
end
local info = redis.call('XINFO', 'STREAM', stream)
local fields = {}
for i = 1, #info, 2 do
    fields[info[i]] = info[i + 1]
end
local added = fields['entries-added']
if not added or not fields['max-deleted-entry-id'] or added > 9007199254740991 then
    return redis.error_reply('SC_REALTIME_COUNTER_CAPACITY')
end
if action == 'initialize' then
    return {fields['last-generated-id'], added}
end
local cursor, read = ARGV[2], tonumber(ARGV[3])
local cm, cs = string.match(cursor, '^(%d+)%-(%d+)$')

-- 业务作用：比较记录与当前游标，保留 Redis ID 的完整整数精度。
-- 参数说明：
-- - id：从 Redis 元数据取得的 ID。
-- 返回：记录严格位于当前游标之后时为 true。
local function after(id)
    local m, s = string.match(id, '^(%d+)%-(%d+)$')
    if not cm or not m then
        error('SC_REALTIME_ID_INVALID')
    end
    if #m ~= #cm then
        return #m > #cm
    end
    if m ~= cm then
        return m > cm
    end
    return #s > #cs or (#s == #cs and s > cs)
end

if not read or read > added then
    return redis.error_reply('SC_REALTIME_BASELINE_REGRESSION')
end
if after(fields['max-deleted-entry-id']) then
    return redis.error_reply('SC_REALTIME_GAP')
end
local first = fields['first-entry']
if (not first or after(first[1])) and added - read ~= fields['length'] then
    return redis.error_reply('SC_REALTIME_GAP')
end
if action == 'observe' then
    return added - read
end
local records = redis.call('XREAD', 'COUNT', ARGV[4], 'STREAMS', stream, cursor)
if not records then
    return {}
end
local maximum = tonumber(ARGV[5])
if not maximum or maximum < 1 then
    return redis.error_reply('SC_REALTIME_RECORD_CAPACITY')
end
-- 在原始响应离开 Redis 前限制每条字段及结构成本，异常大记录不能先耗尽节点内存再隔离。
for _, record in ipairs(records[1][2]) do
    local size = #record[1] + 256
    if #record[2] > 128 then
        return redis.error_reply('SC_REALTIME_RECORD_CAPACITY')
    end
    for _, value in ipairs(record[2]) do
        size = size + #value + 64
    end
    if size > maximum then
        return redis.error_reply('SC_REALTIME_RECORD_CAPACITY')
    end
end
return records[1][2]
