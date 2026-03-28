package com.hmdp.utils;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisIdWorker {
	/*
	 * 开始时间戳
	 */
	private static final long BEGIN_TIMESTAMP=1735689600L;
	private static final int COUNT_BITS=32;
	private StringRedisTemplate stringRedisTemplate;
	public RedisIdWorker(StringRedisTemplate stringRedisTemplate)
	{
		this.stringRedisTemplate=stringRedisTemplate;
	}
	
	public long nextId(String keyPrefix)//用于区分不同业务
	{
		//符号位+时间戳+序列号
		LocalDateTime now=LocalDateTime.now();
		long current=now.toEpochSecond(ZoneOffset.UTC);
		long timestamp=current-BEGIN_TIMESTAMP;
		
		String date=now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
		long count=stringRedisTemplate.opsForValue().increment("icr:"+keyPrefix+":"+date);//自增ID 不会出现空指针
		//自增值存在上限
		//位运算
		return timestamp<<COUNT_BITS|count;
	}
	/*
	 * public static void main(String[] args) { LocalDateTime
	 * time=LocalDateTime.of(2025, 1,1,0,0,0);//基准时间 long second
	 * =time.toEpochSecond(ZoneOffset.UTC); System.out.print(second); }
	 */
}
