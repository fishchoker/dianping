package com.hmdp.utils;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import cn.hutool.core.lang.UUID;

public class SimpleRedisLock implements Ilock{

	@Resource
	private StringRedisTemplate stringRedisTemplate;
	/*
	 * 不同的业务需要不同的锁
	 */
	private String name;
	/*
	 * 构造函数赋值
	 */
	public SimpleRedisLock(String name,StringRedisTemplate stringRedisTemplate) {
		this.name=name;
		this.stringRedisTemplate=stringRedisTemplate;
	}
	private static final String KEY_PREFIX="lock:";
	private static final String ID_PREFIX=UUID.randomUUID().toString(true)+"-";
	@Override
	public boolean tryLock(long timeoutSec)
	{	
		/*
		 * //获取线程标识 long threadId=Thread.currentThread().getId();
		 */
		//将标识变得更复杂用以区分线程
		String threadId=ID_PREFIX+Thread.currentThread().getId();
		//获取锁 自动拆箱有风险
		Boolean success=stringRedisTemplate.opsForValue()
				.setIfAbsent(KEY_PREFIX+name, threadId,timeoutSec,TimeUnit.SECONDS);
		//System.out.print("success: "+success+"\n");
		return Boolean.TRUE.equals(success);
	}
	// 提前读取脚本文件
	private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
	static {
		UNLOCK_SCRIPT = new DefaultRedisScript();
		UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
		UNLOCK_SCRIPT.setResultType(Long.class);
	}
	@Override
	public void unLock() {

		//lua脚本
		stringRedisTemplate.execute(
				UNLOCK_SCRIPT,
				Collections.singletonList(KEY_PREFIX+name),
				ID_PREFIX+Thread.currentThread().getId());
/*		//获取标识 判断是否一致
		String threadId=ID_PREFIX+Thread.currentThread().getId();
		//获取锁中的标识
		String id=stringRedisTemplate.opsForValue().get(KEY_PREFIX+name);
		if(id.equals(threadId)) {
		//释放锁
			stringRedisTemplate.delete(KEY_PREFIX+name);*/
	}
	}
