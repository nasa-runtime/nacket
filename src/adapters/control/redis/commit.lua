for index, key in ipairs(KEYS) do
    local kind = redis.call('TYPE', key).ok
    local expected = (index == 4 or index == 7) and 'stream' or 'string'
    if kind ~= 'none' and kind ~= expected then
        return redis.error_reply('CONTROL_KEY_TYPE')
    end
end
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return redis.error_reply('CONTROL_OWNER_FENCED')
end
local state = redis.call('GET', KEYS[2])
if not state or redis.sha1hex(state) ~= ARGV[2] then
    return redis.error_reply('CONTROL_CAS_CONFLICT')
end
if redis.call('GET', KEYS[3]) ~= ARGV[3] then
    return redis.error_reply('CONTROL_CURSOR_CONFLICT')
end
local hash = redis.sha1hex(ARGV[4])
local position = redis.call('XADD', KEYS[4], 'MAXLEN', '~', 10000, '*', 'sha', hash)
redis.call('SET', KEYS[2], ARGV[4])
redis.call('SET', KEYS[3], ARGV[5])
redis.call('SET', KEYS[5], position)
-- 回复与状态同次提交；失去调用返回值只能查原 requestId，不能把 unknown 当未执行。
redis.call('SET', KEYS[6], ARGV[6], 'PX', ARGV[7])
-- 已处理输入在回复和状态提交之后回收；未处理记录不受 MAXLEN 裁剪。
if ARGV[5] ~= ARGV[3] then
    redis.call('XDEL', KEYS[7], ARGV[5])
end
return {position, hash}
