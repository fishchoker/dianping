package com.hmdp;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Resource;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/*import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;*/
/*import com.hmdp.entity.Shop;
import com.hmdp.service.impl.*;*/

@SpringBootTest
class HmDianPingApplicationTests {
	
	/*
	 * @Resource private ShopServiceImpl shopService;
	 * 
	 * @Resource CacheClient cacheClient;
	 * 
	 * @Resource private RedisIdWorker redisIdWorker; private ExecutorService es
	 * =Executors.newFixedThreadPool(500);
	 */
		/*
		 * @Test void testIdWorker() { CountDownLatch latch=new CountDownLatch(300);
		 * Runnable task = () ->{ for(int i=0;i<100;i++) { long id =
		 * redisIdWorker.nextId("order"); System.out.print("id ="+id); }
		 * latch.countDown(); }; long begin =System.currentTimeMillis(); for(int
		 * i=0;i<300;i++) { es.submit(task); } try { latch.await(); } catch
		 * (InterruptedException e) { e.printStackTrace(); // 或者你可以选择在这里处理线程中断的逻辑 } long
		 * end =System.currentTimeMillis(); System.out.print("time ="+(end-begin)); }
		 */
		
		/*
		 * @Test public void testSaveShop() throws InterruptedException { //预热
		 * //shopService.saveShop2Redis(1L,10L); Shop shop=shopService.getById(1L);
		 * cacheClient.setWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY+ 1L, shop,
		 * 10L, TimeUnit.SECONDS); }
		 */
		 
}
