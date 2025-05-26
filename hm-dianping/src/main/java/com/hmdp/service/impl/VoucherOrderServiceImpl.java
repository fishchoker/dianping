package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;

import cn.hutool.core.bean.BeanUtil;
import lombok.extern.slf4j.Slf4j;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder>
		implements IVoucherOrderService {

	/*
	 * 下单券 voucher_order 需要判断下单时间是否有效 下单时库存是否足够
	 */
	@Resource
	private ISeckillVoucherService seckillVoucherService;// 获取特价券相关的服务
	@Resource
	private RedisIdWorker redisIdWorker;
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	@Resource
	private RedissonClient redissonClient;
	private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
	static {
		SECKILL_SCRIPT = new DefaultRedisScript();
		SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
		SECKILL_SCRIPT.setResultType(Long.class);
	}
	private IVoucherOrderService proxy;
	// 阻塞队列private BlockingQueue<VoucherOrder> orderTasks = new
	// ArrayBlockingQueue<>(1024 * 1024);
	// 线程池
	private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

	@PostConstruct
	private void init() {
		SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
	}

	// 线程任务 内部类
	/*
	 * private class VoucherOrderHandler implements Runnable {
	 * 
	 * @Override public void run() {
	 * 
	 * 任务在类初始化之后就来执行任务
	 * 
	 * while (true) { // 获取队列中的订单信息 // 创建订单 try { VoucherOrder voucherOrder =
	 * orderTasks.take(); handleVoucherOrder(voucherOrder); } catch (Exception e) {
	 * log.error("处理订单异常", e); }
	 * 
	 * } } }
	 */
	private class VoucherOrderHandler implements Runnable {
		String queueName="stream.orders";
		@Override
		public void run() {
			/*
			 * 任务在类初始化之后就来执行任务
			 */
			while (true) {
				// 获取消息队列中的订单信息 没有消息继续下一次
				// 有消息则创建订单
				//ACK确认
				try {
					List<MapRecord<String,Object,Object>> list =stringRedisTemplate.opsForStream().read(
							Consumer.from("g1","c1"),
							StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
							StreamOffset.create(queueName, ReadOffset.lastConsumed())
					);
					if(list == null||list.isEmpty()) {
						continue;
					}
					//解析消息
					MapRecord<String,Object,Object> record=list.get(0);
					Map<Object,Object> value=record.getValue();
					VoucherOrder voucherOrder=BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
					handleVoucherOrder(voucherOrder); 
					//ack确认
					stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
				} catch (Exception e) {
					log.error("处理订单异常", e);
					handlependingList();
				}

			}
		}
		public void handlependingList() {
			// TODO Auto-generated method stub
			while (true) {
				// 获取pendinglist中的订单信息 没有消息继续下一次
				// 有消息则创建订单
				//ACK确认
				try {
					List<MapRecord<String,Object,Object>> list =stringRedisTemplate.opsForStream().read(
							Consumer.from("g1","c1"),
							StreamReadOptions.empty().count(1),
							StreamOffset.create(queueName, ReadOffset.from("0"))
					);
					if(list == null||list.isEmpty()) {
						//pendinglist没有异常消息
						break;
					}
					//解析消息
					MapRecord<String,Object,Object> record=list.get(0);
					Map<Object,Object> value=record.getValue();
					VoucherOrder voucherOrder=BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
					handleVoucherOrder(voucherOrder); 
					//ack确认
					stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());
				} catch (Exception e) {
					log.error("处理pendinglist异常", e);
					try {
					Thread.sleep(20);
					}catch(InterruptedException interruptedException) {
						interruptedException.printStackTrace();
					}
			}
		}
		}
	}

	@Override
	@Transactional
	// 数据库事务
	public Result seckillVoucher(Long voucherId) {
		Long userId = UserHolder.getUser().getId();
		long orderId = redisIdWorker.nextId("order");
		// lua脚本 判断结果是否为0
		Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(),
				userId.toString(), String.valueOf(orderId));
		// 用户有购买资格 消息发出
		int r = result.intValue();
		if (r != 0) {
			return Result.fail(r == 1 ? "库存不足！" : "用户达到购买额度！");
		}

		// 创建阻塞队列
		// orderTasks.add(order);
		// 拿代理对象
		proxy = (IVoucherOrderService) AopContext.currentProxy();
		// 开启异步线程执行下单

		return Result.ok(orderId);

	}

	

	/*
	 * @Override
	 * 
	 * @Transactional //数据库事务 public Result seckillVoucher(Long voucherId) {
	 * 
	 * //查询优惠券 SeckillVoucher voucher=seckillVoucherService.getById(voucherId);
	 * //判断有效期 if((voucher.getBeginTime().isAfter(LocalDateTime.now()))
	 * ||(voucher.getEndTime().isBefore(LocalDateTime.now()))) { //不在有效期内 return
	 * Result.fail("不在有效期内！"); }
	 * 
	 * //判断库存
	 * 
	 * 乐观锁 判断修改库存时的版本（库存）和之前查询到的库存（版本）是否一致
	 * 
	 * 
	 * if(voucher.getStock()<1) { //库存不足 return Result.fail("库存不足！"); }
	 * 
	 * Long userId=UserHolder.getUser().getId(); //lua脚本 判断结果是否为0 Long
	 * result=stringRedisTemplate.execute( SECKILL_SCRIPT, Collections.emptyList(),
	 * voucherId.toString(), userId.toString() ); int r=result.intValue(); if(r!=0)
	 * { return Result.fail(r==1?"库存不足！":"用户达到购买额度！"); } VoucherOrder order=new
	 * VoucherOrder();
	 * 
	 * 把信息保存到阻塞队列
	 * 
	 * long orderId=redisIdWorker.nextId("order"); order.setId(orderId);
	 * order.setUserId(userId); order.setVoucherId(voucherId); //创建阻塞队列
	 * orderTasks.add(order); //拿代理对象
	 * proxy=(IVoucherOrderService)AopContext.currentProxy(); //开启异步线程执行下单
	 * 
	 * return Result.ok(orderId); //对用户id进行判断 //Long
	 * userId=UserHolder.getUser().getId(); //id值相同时就要加锁 不能按照对象判断
	 * 
	 * synchronized (userId.toString().intern()) { //return
	 * this.creatOrder(voucherId);//this对象不是代理对象，没有事务功能 //获取代理对象
	 * IVoucherOrderService proxy=(IVoucherOrderService)AopContext.currentProxy();
	 * return proxy.creatOrder(voucherId);
	 * 
	 * //创建锁对象 获取锁 锁的范围只需要用户
	 * 
	 * SimpleRedisLock lock=new
	 * SimpleRedisLock("order:"+userId,stringRedisTemplate); Boolean
	 * isLock=lock.tryLock(1200);
	 * 
	 * //使用redisson分布式锁
	 * 
	 * RLock lock=redissonClient.getLock("lock:order:"+userId); Boolean
	 * isLock=lock.tryLock(); if(!isLock) { //获取锁失败 一人一单 直接返回失败 return
	 * Result.fail("用户达到购买额度！"); } try { IVoucherOrderService
	 * proxy=(IVoucherOrderService)AopContext.currentProxy();
	 * System.out.println(proxy.getClass().getName()); return
	 * proxy.creatOrder(voucherId); } finally { lock.unlock(); }
	 * 
	 * }
	 */
	public void handleVoucherOrder(VoucherOrder voucherOrder) {
		// TODO Auto-generated method stub
		// 实际上这里不需要考虑锁的问题
		// userId只能从voucherOrder取
		Long userId = voucherOrder.getUserId();
		// 使用redisson分布式锁
		RLock lock = redissonClient.getLock("lock:order:" + userId);
		Boolean isLock = lock.tryLock();
		if (!isLock) { // 获取锁失败 一人一单 直接返回失败 return
			log.error("不允许重复下单");
			return;
		}
		try {
			// 子线程 threadlocal拿不到代理对象 必须提前获取
			/*
			 * IVoucherOrderService proxy=(IVoucherOrderService)AopContext.currentProxy();
			 * System.out.println(proxy.getClass().getName());
			 */

			proxy.creatOrder(voucherOrder);
		} finally {
			lock.unlock();
		}

	}

	/*
	 * 封装成创建order的逻辑 同步锁？事务范围是更新数据库的范围 不建议把锁加载方法上 同一个用户来了才判断并发安全 对用户id加锁
	 */
	@Transactional
	public void creatOrder(VoucherOrder order) {
		// 对用户id进行判断
		Long userId = order.getUserId();
		int count = query().eq("user_id", userId).eq("voucher_id", order.getVoucherId()).count();
		if (count > 0) {
			log.error("用户已经购买过一次");
			return;
		}
		// 扣减库存创建订单
		/*
		 * UPDATE seckill_voucher SET stock = stock - 1 WHERE voucher_id = ?
		 */
		boolean success = seckillVoucherService.update().setSql("stock = stock -1")
				.eq("voucher_id", order.getVoucherId()).gt("stock", 0).update();
		if (!success) {
			log.error("库存不足");
			return;
		}
		/*
		 * VoucherOrder order=new VoucherOrder(); //订单id 用户id 代金券id long
		 * orderId=redisIdWorker.nextId("order"); order.setId(orderId);
		 * order.setUserId(userId); order.setVoucherId(voucherId);
		 */
		save(order);
		// return Result.ok(orderId);

	}
}
