-- 所有读取与确认均验证持久节点代际；围栏不设 TTL，禁止超时删除后让旧实例重新取得权限。
local stream, owner, dlt = KEYS[1], KEYS[2], KEYS[3]
local action, group, incarnation, startup = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
local consumer = incarnation .. ':' .. startup

-- 业务作用：按十进制整数比较控制代际，避免 Lua double 丢失大整数精度。
-- 参数说明：
-- - a：左侧规范十进制整数。
-- - b：右侧规范十进制整数。
-- 返回：左侧严格较大时为 true。
local function greater(a, b)
    return #a > #b or (#a == #b and a > b)
end

-- 业务作用：按 Redis stream ID 的两个整数分量比较处理进度。
-- 参数说明：
-- - a：待比较的记录 ID。
-- - b：已知进度 ID。
-- 返回：左侧严格较新时为 true。
local function after(a, b)
    local am, as = string.match(a, '^(%d+)%-(%d+)$')
    local bm, bs = string.match(b, '^(%d+)%-(%d+)$')
    if not am or not bm then
        error('SC_RECOVERY_INVALID_ID')
    end
    return greater(am, bm) or (am == bm and greater(as, bs))
end

-- 业务作用：提取 Redis 只读信息响应中的命名字段，缺少能力证据时保持关闭。
-- 参数说明：
-- - list：Redis 返回的交替键值列表。
-- - key：需要验证的固定字段。
-- 返回：字段值；不存在则拒绝当前恢复动作。
local function field(list, key)
    for i = 1, #list, 2 do
        if list[i] == key then
            return list[i + 1]
        end
    end
    error('SC_RECOVERY_INFO_MISSING')
end

if redis.call('TYPE', stream).ok ~= 'stream' then
    return redis.error_reply('SC_RECOVERY_STREAM_MISSING')
end
local owner_type = redis.call('TYPE', owner).ok
if owner_type ~= 'none' and owner_type ~= 'hash' then
    return redis.error_reply('SC_RECOVERY_OWNER_TYPE')
end
local groups = redis.call('XINFO', 'GROUPS', stream)
local current = nil
for _, value in ipairs(groups) do
    if field(value, 'name') == group then
        current = value
    end
end
local info = redis.call('XINFO', 'STREAM', stream)
local deleted = field(info, 'max-deleted-entry-id')
local last = field(info, 'last-generated-id')
local added = field(info, 'entries-added')
if added > 9007199254740991 then
    return redis.error_reply('SC_RECOVERY_COUNTER_CAPACITY')
end
-- 脚本执行错误不会回滚先前写入，先确认关键后继命令的 ACL，避免原文已隔离却无确认资格。
if action ~= 'observe' and (not redis.acl_check_cmd('HSET', owner, 'probe', 'probe') or not redis.acl_check_cmd('XACK', stream, group, '0-0')) then
    return redis.error_reply('SC_RECOVERY_ACL')
end

if action == 'acquire' then
    if owner_type == 'none' then
        -- 只有全新的逻辑节点组可以从当前确定末尾建立基线，已有组缺围栏属于状态丢失。
        if current or ARGV[5] ~= '1' then
            return redis.error_reply('SC_RECOVERY_BASELINE_MISSING')
        end
        if #groups >= 256 then
            return redis.error_reply('SC_RECOVERY_GROUP_CAPACITY')
        end
        redis.call('XGROUP', 'CREATE', stream, group, last)
        redis.call('HSET', owner, 'incarnation', incarnation, 'startup', startup, 'group', group, 'cursor', last, 'entriesRead', string.format('%.0f', added))
        return last
    end
    if not current or redis.call('PTTL', owner) ~= -1 then
        return redis.error_reply('SC_RECOVERY_BASELINE_MISSING')
    end
    local previous = redis.call('HGET', owner, 'incarnation')
    if not previous or redis.call('HGET', owner, 'group') ~= group then
        return redis.error_reply('SC_RECOVERY_BASELINE_MISSING')
    end
    if greater(previous, incarnation) or (previous == incarnation and redis.call('HGET', owner, 'startup') ~= startup) then
        return redis.error_reply('SC_RECOVERY_FENCED')
    end
    -- 裁剪超过未读水位时必须重建业务基线，不能把组重置到末尾并声称恢复成功。
    if after(deleted, field(current, 'last-delivered-id')) then
        return redis.error_reply('SC_RECOVERY_GAP')
    end
    redis.call('HSET', owner, 'incarnation', incarnation, 'startup', startup)
    return field(current, 'last-delivered-id')
end

-- 新代际取得围栏后旧实例既不能取走新记录，也不能确认继任者的 pending。
if not current or owner_type ~= 'hash' or redis.call('PTTL', owner) ~= -1 then
    return redis.error_reply('SC_RECOVERY_BASELINE_MISSING')
end
if redis.call('HGET', owner, 'group') ~= group or redis.call('HGET', owner, 'incarnation') ~= incarnation or redis.call('HGET', owner, 'startup') ~= startup then
    return redis.error_reply('SC_RECOVERY_FENCED')
end
if after(deleted, field(current, 'last-delivered-id')) then
    return redis.error_reply('SC_RECOVERY_GAP')
end
local cursor = redis.call('HGET', owner, 'cursor')
local read = tonumber(redis.call('HGET', owner, 'entriesRead'))
if cursor ~= field(current, 'last-delivered-id') or not read or read > added then
    return redis.error_reply('SC_RECOVERY_BASELINE_MISMATCH')
end
local first = field(info, 'first-entry')
-- XTRIM 不更新删除高水位；首条已经越过游标时，用累计写入与已读数量核对实际剩余记录。
if (not first or after(first[1], cursor)) and added - read ~= field(info, 'length') then
    return redis.error_reply('SC_RECOVERY_GAP')
end

if action == 'observe' then
    -- 完整 PEL 最多覆盖正常读取的一个批次；异常扩张不进行无限遍历，也不补造完整零积压。
    local count = field(current, 'pending')
    if count > 128 then
        return redis.error_reply('SC_RECOVERY_OBSERVATION_CAPACITY')
    end
    local pending = redis.call('XPENDING', stream, group, '-', '+', 128)
    if #pending ~= count then
        return redis.error_reply('SC_RECOVERY_OBSERVATION_INCOMPLETE')
    end
    local idle = 0
    for _, item in ipairs(pending) do
        idle = math.max(idle, item[3])
    end
    return {added - read, count, idle}
end

if action == 'read' then
    local maximum = tonumber(ARGV[6])
    if not maximum or maximum < 1 then
        return redis.error_reply('SC_RECOVERY_RECORD_CAPACITY')
    end
    -- 业务作用：在认领或推进组游标前限制完整原始响应，容量失败不得改变消费责任。
    -- 参数说明：
    -- - entries：本次从持久 stream 读取的原字段集合。
    -- 返回：每条记录均可被节点预留预算覆盖；超限抛出固定错误。
    local function bounded(entries)
        for _, record in ipairs(entries) do
            local size = #record[1] + 256
            if #record[2] > 128 then
                error('SC_RECOVERY_RECORD_CAPACITY')
            end
            for _, value in ipairs(record[2]) do
                size = size + #value + 64
            end
            if size > maximum then
                error('SC_RECOVERY_RECORD_CAPACITY')
            end
        end
    end
    local count = tonumber(ARGV[5])
    if not count or count < 1 or count > 128 then
        return redis.error_reply('SC_RECOVERY_CAPACITY')
    end
    local pending = redis.call('XPENDING', stream, group, '-', '+', count)
    if #pending > 0 then
        local result = {}
        -- 独占围栏允许立即认领；每条先确认原文存在，不能通过 XCLAIM 自动清理丢失证据。
        for _, item in ipairs(pending) do
            local record = redis.call('XRANGE', stream, item[1], item[1])
            if #record ~= 1 then
                return redis.error_reply('SC_RECOVERY_GAP')
            end
            table.insert(result, record[1])
        end
        bounded(result)
        for _, item in ipairs(pending) do
            redis.call('XCLAIM', stream, group, consumer, 0, item[1], 'JUSTID')
        end
        return {result, #result}
    end
    -- 同一脚本内先检查候选再推进组水位，不能依赖 Lua 错误回滚已经产生的 PEL。
    bounded(redis.call('XRANGE', stream, '(' .. cursor, '+', 'COUNT', count))
    local records = redis.call('XREADGROUP', 'GROUP', group, consumer, 'COUNT', count, 'STREAMS', stream, '>')
    if not records then
        return {{}, 0}
    end
    local entries = records[1][2]
    redis.call('HSET', owner, 'cursor', entries[#entries][1], 'entriesRead', string.format('%.0f', read + #entries))
    return {entries, 0}
end

if action == 'ack' or action == 'reject' then
    local id = ARGV[5]
    local pending = redis.call('XPENDING', stream, group, id, id, 1)
    if #pending == 0 then
        return 0
    end
    if pending[1][2] ~= consumer then
        return redis.error_reply('SC_RECOVERY_FENCED')
    end
    if action == 'reject' then
        if not redis.acl_check_cmd('XADD', dlt, '*', 'probe', 'probe') then
            return redis.error_reply('SC_RECOVERY_DLT_ACL')
        end
        local destination_type = redis.call('TYPE', dlt).ok
        if destination_type ~= 'none' and destination_type ~= 'stream' then
            return redis.error_reply('SC_RECOVERY_DLT_TYPE')
        end
        local record = redis.call('XRANGE', stream, id, id)
        if #record ~= 1 then
            return redis.error_reply('SC_RECOVERY_GAP')
        end
        local size = 0
        for _, value in ipairs(record[1][2]) do
            size = size + #value
        end
        if size > tonumber(ARGV[6]) then
            return redis.error_reply('SC_RECOVERY_DLT_CAPACITY')
        end
        -- 隔离原文和确认放在同一槽脚本内，响应丢失重试发现 PEL 已空时不会重复隔离。
        redis.call('XADD', dlt, '*', 'sourceId', id, 'sourceStream', stream, 'sourceGroup', group, 'reason', 'record_rejected', unpack(record[1][2]))
    end
    return redis.call('XACK', stream, group, id)
end

if action == 'cleanup' then
    local consumers = redis.call('XINFO', 'CONSUMERS', stream, group)
    -- 仅回收同一稳定组中没有责任的旧 consumer；不删除组、围栏或其它逻辑节点的状态。
    for _, value in ipairs(consumers) do
        local name = field(value, 'name')
        if name ~= consumer and field(value, 'pending') == 0 then
            redis.call('XGROUP', 'DELCONSUMER', stream, group, name)
        end
    end
    return 1
end
return redis.error_reply('SC_RECOVERY_ACTION')
