package com.hmdp.service.impl;

import com.hmdp.entity.Shop;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import javax.annotation.Resource;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.Metrics;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.CacheClient;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import cn.hutool.json.JSONObject;
/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	@Resource CacheClient cacheClient;
	@Override
	public Result queryById(Long id)
	{
		//缓存穿透代码封装
		//Shop shop=queryWithMutex(id);
		
		  Shop shop=cacheClient.queryWithPassThrough(
		  RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById,RedisConstants.
		  CACHE_SHOP_TTL,TimeUnit.MINUTES);
		 
		//Shop shop=queryWithLogicalExpire(id);
		//if(queryWithPassThrough(id)==null||queryWithMutex(id)==null)
		/*
		 * //需要数据预热 Shop shop=cacheClient.queryWithLogicalExpire(
		 * RedisConstants.CACHE_SHOP_KEY,id,Shop.class,this::getById,20L,TimeUnit.
		 * SECONDS);
		 */
		if(shop==null)
			{
			return Result.fail("店铺不存在！");
			}
		
		return Result.ok(shop);

	}
	@Override
	@Transactional
	public Result update(Shop shop)
	{
		//先对id做判断
		Long id=shop.getId();
		if(id==null)
		{
			return Result.fail("店铺id不能为空！");
		}
		//更新数据库
		updateById(shop);
		//删除缓存
		stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+id);
		return Result.ok();
	}
	@Override
	public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
		// 是否需要坐标查询
		if(x==null||y==null) {
			//数据库查
			Page<Shop> page =query()
					.eq("type_id",typeId)
					.page(new Page<>(current,SystemConstants.DEFAULT_PAGE_SIZE));
			return Result.ok(page.getRecords());
		}
		//分页参数计算
		int from=(current-1)*SystemConstants.DEFAULT_PAGE_SIZE;
		int end=current*SystemConstants.DEFAULT_PAGE_SIZE;
//		//查询  shopId distance
//		String key=RedisConstants.SHOP_GEO_KEY+typeId;
//		GeoResults<RedisGeoCommands.GeoLocation<String>> results =stringRedisTemplate.opsForGeo()
//		.search(key, GeoReference.fromCoordinate(x,y),new Distance(5000)
//				,RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
//				);
		// 查询 shopId 和距离
		String key = RedisConstants.SHOP_GEO_KEY + typeId;
		GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
		    .radius(key, new Circle(new Point(x, y), new Distance(5000, Metrics.METERS)),
		        RedisGeoCommands.GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().limit(end)
		    );

		//解析出Id
		if(results==null){
	        return Result.ok(Collections.emptyList());
	    }
	    List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();//0~end
	    //截取from到end 不需要集合拷贝
	    List<Long> ids= new ArrayList<>(list.size());
	    Map<String,Distance> distanceMap=new HashMap<>(list.size());
	    list.stream().skip(from).forEach(result ->{
	    	String shopIdstr=result.getContent().getName();
	    	ids.add(Long.valueOf(shopIdstr));
	    	Distance distance=result.getDistance();
	    	distanceMap.put(shopIdstr, distance);
	    });;
		//查询到店铺 根据ID批量查
	    String idStr=StrUtil.join(",", ids);
	    if(idStr.isEmpty())
	    {
	    	return Result.ok(Collections.emptyList());
	    }
	    List<Shop> shops=query().in("id", ids).last("ORDER BY FIELD(ID,"+idStr+")").list();
	    for(Shop shop:shops) {
	    	shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
	    }
		return Result.ok(shops);
	}

	/*
	 * //实现互斥锁 private boolean getLock(String key)//redis的锁就是key { Boolean
	 * flag=stringRedisTemplate.opsForValue().setIfAbsent(key,
	 * "1",10,TimeUnit.SECONDS);//获取锁 值随便设 //需要转成基本类型 不可以自动拆箱（可能空指针！ return
	 * BooleanUtil.isTrue(flag); } //把锁删掉 private void unLock(String key) {
	 * stringRedisTemplate.delete(key); }
	 */
	/*
	 * //封装好的缓存击穿逻辑 public Shop queryWithMutex(Long id) { //互斥锁解决缓存击穿 String
	 * shopJson=stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY+
	 * id); if(StrUtil.isNotBlank(shopJson)) { return
	 * JSONUtil.toBean(shopJson,Shop.class); } if(shopJson!=null)//检查是否为空！ { return
	 * null; } //要获取互斥锁 实现缓存重建 //获取互斥锁 String
	 * lockkey=RedisConstants.LOCK_SHOP_KEY+id; Shop shop=null; try { boolean
	 * islock=getLock(lockkey); //判断是否成功获取锁 while(!islock) { //失败 休眠并重试
	 * Thread.sleep(50); // 休眠50毫秒
	 * 
	 * //重试 return queryWithMutex(id);
	 * 
	 * } //成功 将数据写入redis shop=getById(id);//访问数据库 Thread.sleep(200);//模拟重建延时
	 * if(shop==null) {
	 * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",
	 * RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES); return null; } //存在 写入redis
	 * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,
	 * JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
	 * //返回 //需要设置超时时间 }catch (InterruptedException e) { e.printStackTrace(); //
	 * 捕获并处理 InterruptedException 异常 } //释放互斥锁 unLock(lockkey); return shop; }
	 */
	//封装好的缓存穿透逻辑
	/*
	 * public Shop queryWithPassThrough(Long id) { //从redis查商户缓存 id作为店铺的key String
	 * shopJson=stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY+
	 * id); //System.out.printf("shopJson"+shopJson); //是否存在
	 * if(StrUtil.isNotBlank(shopJson))//isNotBlank只有存在有效字符串时返回ture { //存在 直接返回
	 * //Shop shop=JSONUtil.toBean(shopJson,Shop.class);
	 * 
	 * return JSONUtil.toBean(shopJson,Shop.class); } //需要把空值判断逻辑放在if之后
	 * if(shopJson!=null)//检查是否为空！ { //return Result.fail("店铺不存在！"); return null; }
	 * //不存在 返回错误 //将空值写入redis解决缓存穿透 //有效期设短一点 //解决缓存击穿 在数据库查询之前先申请互斥锁
	 * 
	 * Shop shop=getById(id);//访问数据库 if(shop==null) {
	 * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",
	 * RedisConstants.CACHE_NULL_TTL,TimeUnit.MINUTES); //return
	 * Result.fail("店铺不存在！"); return null; } //存在 写入redis
	 * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,
	 * JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL,TimeUnit.MINUTES);
	 * //返回 //需要设置超时时间 return shop; }
	 */
	/*
	 * public void saveShop2Redis(Long id,Long seconds)//把shop存储到redis中 { //查询店铺数据
	 * Shop shop=getById(id); try { //Thread.sleep(200);//模拟重建延时 } catch(Exception
	 * e) { e.printStackTrace(); } //封装成逻辑过期 RedisData data= new RedisData();
	 * data.setData(shop);
	 * data.setExpireTime(LocalDateTime.now().plusSeconds(seconds)); //写入redis
	 * stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,
	 * JSONUtil.toJsonStr(data));//物理永久有效 return ; }
	 */
	/*
	 * public Shop queryWithLogicalExpire(Long id) { //从redis查商户缓存 id作为店铺的key String
	 * shopJson=stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY+
	 * id); //是否存在 if(StrUtil.isEmpty(shopJson))//未命中返回 { return null; } //存在 判断是否过期
	 * 
	 * //反序列化为对象 RedisData redisdata=JSONUtil.toBean(shopJson, RedisData.class);
	 * Shop shop=JSONUtil.toBean((JSONObject)redisdata.getData(),Shop.class);
	 * LocalDateTime expireTime=redisdata.getExpireTime(); //判断是否过期
	 * if(expireTime.isAfter(LocalDateTime.now())) { return shop; } //未过期 直接返回 //过期
	 * 进行缓存重建 //获取互斥锁 String lockkey=RedisConstants.LOCK_SHOP_KEY +id; boolean
	 * islock=getLock(lockkey); //判断是否成功获取锁 if(islock) { //获取成功 新线程缓存重建 //使用线程池
	 * CACHE_REBUILD_EXECUTOR.submit(() ->{ try { this.saveShop2Redis(id,20L);
	 * }catch (Exception e) { throw new RuntimeException(e); // 捕获并处理
	 * InterruptedException 异常 }finally { //释放锁 unLock(lockkey); } }); }
	 * 
	 * //获取失败 //返回过期信息 return shop; } private static final ExecutorService
	 * CACHE_REBUILD_EXECUTOR= Executors.newFixedThreadPool(10);
	 */
}
