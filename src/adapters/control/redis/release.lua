-- 只归还本实例仍拥有的锁，迟到停机不能删除新 controller 的所有权。
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
