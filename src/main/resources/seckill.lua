---参数列表---
-- 读取redis的key来判断库存
-- 取到优惠券ID
local voucherId =ARGV[1]
--用户ID
local userId =ARGV[2]
local orderId =ARGV[3]
--库存key
local stockKey ='seckill:stock:'..voucherId
--订单key
local orderKey ='seckill:order:'..voucherId
--开始实现
--取库存值判断是否>0
if(tonumber(redis.call('get',stockKey))<=0) then
	--库存不足返回1
	return 1
end
--一人一单
if(redis.call('sismember',orderKey,userId)==1) then
	return 2
end
redis.call('incrby',stockKey,-1)
redis.call('sadd',orderKey,userId)
-- 改为写入 Outbox 队列，由 Kafka 转发器读取
redis.call('rpush','queue:seckill:pending',orderId .. ':' .. userId .. ':' .. voucherId)

return 0