package com.hmdp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
/**
 * Redisson配置
 */
@Configuration
public class RedisConfig {
	@Bean
	public RedissonClient redissonClient() {
		//配置类
		Config config=new Config();
		//单点redis地址 使用configuseClusterServers为集群地址
		config.useSingleServer().setAddress("redis://127.0.0.1:6379");
		//创建redisson客户端
		return Redisson.create(config);
		
	}

}
