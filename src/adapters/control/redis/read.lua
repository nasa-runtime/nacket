local state = redis.call('GET', KEYS[1])
if not state then
    return redis.error_reply('CONTROL_BASELINE_MISSING')
end
local cursor = redis.call('GET', KEYS[2])
local position = redis.call('GET', KEYS[3])
if not cursor or not position then
    return redis.error_reply('CONTROL_CHECKPOINT_MISSING')
end
local record = redis.call('XRANGE', KEYS[4], position, position, 'COUNT', 1)
local hash = redis.sha1hex(state)
-- 快照与日志最后提交标记必须共同存在，不能把脚本中断后的部分写入当作完整控制基线。
if #record ~= 1 or #record[1][2] ~= 2 or record[1][2][1] ~= 'sha' or record[1][2][2] ~= hash then
    return redis.error_reply('CONTROL_CHECKPOINT_INCONSISTENT')
end
return {state, cursor, position, hash}
