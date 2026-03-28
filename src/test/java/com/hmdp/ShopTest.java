package com.hmdp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisCommands;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.lettuce.LettuceConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;

import io.lettuce.core.GeoSearch;

@SpringBootTest
public class ShopTest {

		@Resource
		private ShopServiceImpl shopService;

		@Resource CacheClient cacheClient;

		@Resource
		private RedisIdWorker redisIdWorker;
		private ExecutorService es = Executors.newFixedThreadPool(500);

		@Resource
		private StringRedisTemplate stringRedisTemplate;
		@Test
		void testGeoSearchDirect() {
		    Double x= 120.0001;
		    Double y= 30.2321;
		    try {
				String key=RedisConstants.SHOP_GEO_KEY+1;
				GeoResults<RedisGeoCommands.GeoLocation<String>> results =stringRedisTemplate.opsForGeo()
				.search(key, GeoReference.fromCoordinate(x,y),new Distance(5000)
						,RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(10)
						);
		        System.out.println("GEOSEARCH命令可用");
		    } catch (Exception e) {
		        System.out.println("命令执行失败: " + e.getMessage());
		    }
		}


}
