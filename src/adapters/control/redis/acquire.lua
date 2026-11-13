-- 控制键类型先全部确认，任何可预见结构冲突都不能发生在取得新 owner 之后。
for index, key in ipairs(KEYS) do
    local kind = redis.call('TYPE', key).ok
    local expected = index == 4 and 'stream' or 'string'
    if kind ~= 'none' and kind ~= expected then
        return redis.error_reply('CONTROL_KEY_TYPE')
    end
end
local owner = redis.call('GET', KEYS[1])
if owner then
    if owner == ARGV[1] then
        redis.call('PEXPIRE', KEYS[1], ARGV[3])
        return owner
    end
    return false
end
local counter = redis.call('GET', KEYS[2])
local state = redis.call('GET', KEYS[3])
if not state then
    if counter or redis.call('EXISTS', KEYS[4], KEYS[5], KEYS[6]) ~= 0 or ARGV[4] ~= 'initialize' then
        return redis.error_reply('CONTROL_BASELINE_MISSING')
    end
elseif not counter then
    return redis.error_reply('CONTROL_COUNTER_MISSING')
end
if counter and (not string.match(counter, '^%d+$') or string.len(counter) > 18) then
    return redis.error_reply('CONTROL_COUNTER_INVALID')
end
if not state then
    -- 首次作用域建立必须显式批准，已有计数器但丢失状态不能作为新集群自动初始化。
    local hash = redis.sha1hex(ARGV[5])
    local position = redis.call('XADD', KEYS[4], '*', 'sha', hash)
    redis.call('SET', KEYS[3], ARGV[5])
    redis.call('SET', KEYS[5], '0-0')
    redis.call('SET', KEYS[6], position)
end
redis.call('INCR', KEYS[2])
local term = redis.call('GET', KEYS[2])
local token = term .. ':' .. ARGV[2]
redis.call('SET', KEYS[1], token, 'PX', ARGV[3])
return token
