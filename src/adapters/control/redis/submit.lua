local kind = redis.call('TYPE', KEYS[1]).ok
if kind ~= 'none' and kind ~= 'stream' then
    return redis.error_reply('CONTROL_KEY_TYPE')
end
if string.len(ARGV[1]) > 262144 or redis.call('XLEN', KEYS[1]) >= 128 then
    return redis.error_reply('CONTROL_INPUT_CAPACITY')
end
return redis.call('XADD', KEYS[1], '*', 'request', ARGV[1])
