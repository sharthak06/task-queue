export const CLAIM_SCRIPT = `
local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[2], 'LIMIT', 0, 1)
if #items == 0 then return nil end
local task = items[1]
redis.call('ZREM', KEYS[1], task)
redis.call('ZADD', KEYS[2], ARGV[1], task)
return task
`;

export const FAIL_SCRIPT = `
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
if removed == 0 then return 0 end
local task = cjson.decode(ARGV[1])
task['attempts'] = task['attempts'] + 1
if task['attempts'] >= task['maxAttempts'] then
  redis.call('XADD', KEYS[3], '*',
    'taskId', task['id'],
    'type', task['type'],
    'payload', cjson.encode(task['payload']),
    'error', ARGV[4],
    'attempts', tostring(task['attempts'])
  )
  return 2
end
local delay = tonumber(ARGV[3]) * (2 ^ task['attempts'])
local score = tonumber(ARGV[2]) + delay
redis.call('ZADD', KEYS[2], score, cjson.encode(task))
return 1
`;
