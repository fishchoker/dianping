package com.hmdp.utils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import com.hmdp.entity.Shop;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
/*
 * 缓存工具类
 */
@Slf4j
@Component
public class CacheClient {

	private StringRedisTemplate stringRedisTemplate;
	
	public CacheClient(StringRedisTemplate stringRedisTemplate)
	{
		this.stringRedisTemplate=stringRedisTemplate;
	}
	/*
	 * 
	 */
	public void set(String key,Object value,Long time,TimeUnit unit)
	{
		/*
		 * value需要序列化
		 */
		stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
	}
	
	public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit unit)
	{
		/*
		 * 写入redis 添加逻辑过期字段
		 * 把value封装到redisdata
		 */
		RedisData redisData=new RedisData();
		redisData.setData(value);
		redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
		stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData),time,unit);
	}
	/*
	 * 缓存穿透 返回值不确定时使用泛型
	 */
	public <R,ID> R queryWithPassThrough(
			String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit)
	{
		String key=keyPrefix+id;
		//从redis查商户缓存 id作为店铺的key
				String Json=stringRedisTemplate.opsForValue().get(key);
				//System.out.printf("shopJson"+shopJson);
				//是否存在
				if(StrUtil.isNotBlank(Json))//isNotBlank只有存在有效字符串时返回ture
				{
					//存在 直接返回
					//Shop shop=JSONUtil.toBean(shopJson,Shop.class);

					return JSONUtil.toBean(Json,type);
				}
				//需要把空值判断逻辑放在if之后
				if(Json!=null)//检查是否为空！
				{
					return null;
				}
				//不存在 返回错误
				//将空值写入redis解决缓存穿透
				//有效期设短一点
				//解决缓存击穿 在数据库查询之前先申请互斥锁
				/*
				 * 如何访问数据库不确定 需要提供方法
				 */
				R r=dbFallback.apply(id);//访问数据库
				if(r==null)
				{
					stringRedisTemplate.opsForValue().set(key,"",
							RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES);
					//return Result.fail("店铺不存在！");
					return null;
				}
				//存在 写入redis
				this.set(key, r, time, unit);	//返回
				//需要设置超时时间
				return r;
	}
	public <R,ID> R queryWithLogicalExpire(
			String keyPrefix,ID id,Class<R> type,Function<ID,R> dbFallback,Long time,TimeUnit unit)
	{
		String key=keyPrefix+id;
		//从redis查商户缓存 id作为店铺的key
				String Json=stringRedisTemplate.opsForValue().get(key);
				//是否存在
				if(StrUtil.isEmpty(Json))//未命中返回
				{
					return null;
				}
				//存在 判断是否过期
				//反序列化为对象
				RedisData redisdata=JSONUtil.toBean(Json, RedisData.class);
				R r=JSONUtil.toBean((JSONObject)redisdata.getData(),type);
				LocalDateTime expireTime=redisdata.getExpireTime();				
				//判断是否过期
				if(expireTime.isAfter(LocalDateTime.now())) {
					return r;
				}
				//未过期 直接返回
				//过期 进行缓存重建
				//获取互斥锁
				String lockkey=RedisConstants.LOCK_SHOP_KEY +id;				
				boolean islock=getLock(lockkey);
				//判断是否成功获取锁
				if(islock) {
					//获取成功 新线程缓存重建
					//使用线程池
					CACHE_REBUILD_EXECUTOR.submit(() ->{
						try {
							//重建缓存
							//查询数据库
							R r1=dbFallback.apply(id);
							//写入redis
							this.setWithLogicalExpire(key, r1, time, unit);
						}catch (Exception e) {
						    throw new RuntimeException(e); // 捕获并处理 InterruptedException 异常
						}finally {
							//释放锁
							unLock(lockkey);
						}
					});		
					}
				
				//获取失败
				//返回过期信息
				return r;
	}
	private static final ExecutorService CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);	
	//实现互斥锁
	private boolean getLock(String key)//redis的锁就是key
	{
		Boolean flag=stringRedisTemplate.opsForValue().setIfAbsent(key, "1",10,TimeUnit.SECONDS);//获取锁 值随便设
		//需要转成基本类型 不可以自动拆箱（可能空指针！
		return BooleanUtil.isTrue(flag);
	}
	//把锁删掉
	private void unLock(String key)
	{
		stringRedisTemplate.delete(key);
	}
}
