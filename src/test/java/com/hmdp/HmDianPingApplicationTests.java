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
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.hmdp.entity.Shop;
import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;

@SpringBootTest
class HmDianPingApplicationTests {

	@Resource
	private ShopServiceImpl shopService;

	@Resource CacheClient cacheClient;

	@Resource
	private RedisIdWorker redisIdWorker;
	private ExecutorService es = Executors.newFixedThreadPool(500);

	@Resource
	private StringRedisTemplate stringRedisTemplate;

//	 @Test 
//	 void testIdWorker() { 
//		 CountDownLatch latch=new CountDownLatch(300);
//		 redisIdWorker.nextId("order"); 
//	 	latch.countDown(); }; 
//	 	long begin =System.currentTimeMillis(); 
//	 	for(int i=0;i<300;i++) { 
//	 		es.submit(task); 
//	 		} 
//	 	try { 
//	 		latch.await(); 
//	 	} catch(InterruptedException e) {
//	 		e.printStackTrace(); // 或者你可以选择在这里处理线程中断的逻辑 } long
//	 		end =System.currentTimeMillis(); 
//	 		System.out.print("time ="+(end-begin)); 
//	 		}
//}
//}


	
//	  @Test 
//	  public void testSaveShop() throws InterruptedException { //预热
//	  //shopService.saveShop2Redis(1L,10L); Shop shop=shopService.getById(1L);
//	  cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+ 1L, shop,
//	  10L, TimeUnit.SECONDS); 
//	  }

	@Test
	void loadShopData() {
		
		// 查询店铺信息
		List<Shop> list = shopService.list();
		// 分组 typeId map
		Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(shop -> shop.getTypeId()));
		// 分批完成写入redis
		for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
			// 获取类型ID
			Long typeId = entry.getKey();
			// 获取同类店铺的集合
			List<Shop> value = entry.getValue();
			// 写入redis GEPADD KEY 经度 纬度 member
			String key = RedisConstants.SHOP_GEO_KEY + typeId;
//			for(Shop shop :value) {
//				stringRedisTemplate.opsForGeo().add(key,new point(shop.getX(),shop.getY()),shop.getId());
//			}
			List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList(value.size());
			for (Shop shop : value) {
				locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),
						new Point(shop.getX(), shop.getY())));
			}
			// 批量写入redis
			stringRedisTemplate.opsForGeo().add(key, locations);
		}
	}
}
