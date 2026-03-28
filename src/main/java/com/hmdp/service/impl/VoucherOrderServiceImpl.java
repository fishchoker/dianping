package com.hmdp.service.impl;

import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;

import cn.hutool.core.codec.Base64;
import cn.hutool.extra.qrcode.QrCodeUtil;
import cn.hutool.extra.qrcode.QrConfig;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.imageio.ImageIO;

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

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

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
	private IVoucherService voucherService;// 获取优惠券相关的服务
	@Resource
	private RedisIdWorker redisIdWorker;
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	@Resource
	private RedissonClient redissonClient;
	private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
	static {
		SECKILL_SCRIPT = new DefaultRedisScript<>();
		SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
		SECKILL_SCRIPT.setResultType(Long.class);
	}
	private IVoucherOrderService proxy;
	@Resource
	private ObjectMapper objectMapper;
	@Value("${server.port}")
	private int serverPort;
	@Resource
	private ApplicationContext applicationContext;
	// 阻塞队列private BlockingQueue<VoucherOrder> orderTasks = new
	// ArrayBlockingQueue<>(1024 * 1024);
	// 线程池
	private static ExecutorService SECKILL_ORDER_EXECUTOR;
	@Value("${order.consumer.concurrency:4}")
	private int consumerConcurrency;
	@Value("${order.stream.enabled:false}")
	private boolean streamEnabled;

	@PostConstruct
	private void init() {
		if (!streamEnabled) {
			log.info("Redis Stream consumer disabled (order.stream.enabled=false). Kafka consumption active.");
			return;
		}
		// 恢复 Redis Stream 的消费者线程
		if (consumerConcurrency < 1) {
			consumerConcurrency = 1;
		}
		SECKILL_ORDER_EXECUTOR = Executors.newFixedThreadPool(consumerConcurrency);
		for (int i = 0; i < consumerConcurrency; i++) {
			SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
		}
	}

	// 线程任务 内部类
	/*
	 * 使用阻塞队列的早期实现，已废弃，保留以备回退
	 * private class VoucherOrderHandler implements Runnable {
	 *     @Override public void run() {
	 *         while (true) {
	 *             try { VoucherOrder voucherOrder = orderTasks.take();
	 *                 handleVoucherOrder(voucherOrder);
	 *             } catch (Exception e) { log.error("处理订单异常", e); }
	 *         }
	 *     }
	 * }
	 */
	// 恢复 Redis Stream 的消费者实现
	 private class VoucherOrderHandler implements Runnable {
	     String queueName="stream.orders";
	     String groupName="g1";
	     String consumerName = buildConsumerName();
	     
	     private VoucherOrder parseVoucherOrder(Map<Object,Object> value) {
	     	if (value == null || value.isEmpty()) {
	     		return null;
	     	}
	     	try {
	     		Object idRaw = value.get("id");
	     		Object userIdRaw = value.get("userId");
	     		Object voucherIdRaw = value.get("voucherId");
	     		if (idRaw == null || userIdRaw == null || voucherIdRaw == null) {
	     			log.error("订单消息缺少必要字段: {}", value);
	     			return null;
	     		}
	     		long id = Long.parseLong(String.valueOf(idRaw));
	     		long userId = Long.parseLong(String.valueOf(userIdRaw));
	     		long voucherId = Long.parseLong(String.valueOf(voucherIdRaw));
	     		VoucherOrder order = new VoucherOrder();
	     		order.setId(id);
	     		order.setUserId(userId);
	     		order.setVoucherId(voucherId);
	     		return order;
	     	} catch (Exception ex) {
	     		log.error("解析订单消息失败: {}", value, ex);
	     		return null;
	     	}
	     }
	     
	     private String buildConsumerName() {
	     	// 唯一消费者名：主机名-端口-短UUID
	     	String host = "unknown";
	     	try {
	     		host = InetAddress.getLocalHost().getHostName();
	     	} catch (UnknownHostException ignored) {}
			String portPart = String.valueOf(VoucherOrderServiceImpl.this.serverPort);
	     	String uuid = UUID.randomUUID().toString().substring(0, 8);
	     	return "c-" + host + "-" + portPart + "-" + uuid;
	     }
	     
	     private void sendToDlq(String reason, Map<Object,Object> value, String recordId, Throwable error) {
	     	try {
	     		java.util.Map<String,String> body = new java.util.HashMap<>();
	     		body.put("reason", String.valueOf(reason));
	     		if (recordId != null) body.put("recordId", String.valueOf(recordId));
	     		if (error != null) {
	     			body.put("errorClass", String.valueOf(error.getClass().getName()));
	     			if (error.getMessage() != null) body.put("errorMessage", String.valueOf(error.getMessage()));
	     		}
	     		if (value != null) {
	     			for (java.util.Map.Entry<Object,Object> e : value.entrySet()) {
	     				body.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
	     			}
	     		}
	     		org.springframework.data.redis.connection.stream.MapRecord<String,String,String> record =
	     			org.springframework.data.redis.connection.stream.MapRecord.create("stream.orders.dlq", body);
	     		stringRedisTemplate.opsForStream().add(record);
	     		System.out.println("写入DLQ, body=" + body);
	     	} catch (Exception ex) {
	     		log.error("写入DLQ失败 recordId={}", recordId, ex);
	     	}
	     }
	     
	     @Override
	     public void run() {
	         while (true) {
	             try {
	                 List<MapRecord<String,Object,Object>> list = stringRedisTemplate.opsForStream().read(
	                         Consumer.from(groupName, consumerName),
	                         StreamReadOptions.empty().count(32).block(Duration.ofMillis(200)),
	                         StreamOffset.create(queueName, ReadOffset.lastConsumed())
	                 );
	                 if(list == null||list.isEmpty()) {
	                     continue;
	                 }
	                 for (MapRecord<String,Object,Object> record : list) {
	                 	Map<Object,Object> value=record.getValue();
	                 	try {
	                 		VoucherOrder voucherOrder=parseVoucherOrder(value);
	                 		if (voucherOrder != null) {
	                 			handleVoucherOrder(voucherOrder);
	                 			stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,record.getId());
	                 		} else {
	                 			log.warn("无法解析的订单消息，转入DLQ并ACK, id={}, value={}", record.getId(), value);
	                 			sendToDlq("parse_failed", value, record.getId().getValue(), null);
	                 			stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,record.getId());
	                 		}
	                 	} catch (Exception exOne) {
	                 		log.error("处理订单消息失败，转入DLQ并ACK, id={}", record.getId(), exOne);
	                 		sendToDlq("handle_failed", value, record.getId().getValue(), exOne);
	                 		stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,record.getId());
	                 	}
	                 }
	             } catch (Exception e) {
	                 log.error("处理订单异常", e);
	                 handlependingList();
	             }
	         }
	     }
	     public void handlependingList() {
	         while (true) {
	             try {
	                 List<MapRecord<String,Object,Object>> list = stringRedisTemplate.opsForStream().read(
	                         Consumer.from(groupName, consumerName),
	                         StreamReadOptions.empty().count(32),
	                         StreamOffset.create(queueName, ReadOffset.from("0"))
	                 );
	                 if(list == null||list.isEmpty()) {
	                     break;
	                 }
	             for (MapRecord<String,Object,Object> record : list) {
	             	Map<Object,Object> value=record.getValue();
	             	try {
	             		VoucherOrder voucherOrder=parseVoucherOrder(value);
	             		if (voucherOrder != null) {
	             			handleVoucherOrder(voucherOrder);
	             			stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,record.getId());
	             		} else {
	             			log.warn("无法解析的pending订单消息，转入DLQ并ACK, id={}, value={}", record.getId(), value);
	             			sendToDlq("pending_parse_failed", value, record.getId().getValue(), null);
	             			stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,record.getId());
	             		}
	             	} catch (Exception exOne) {
	             		log.error("处理pending订单消息失败，转入DLQ并ACK, id={}", record.getId(), exOne);
	             		sendToDlq("pending_handle_failed", value, record.getId().getValue(), exOne);
	             		stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,record.getId());
	             	}
	             }
	             } catch (Exception e) {
	                 log.error("处理pendinglist异常", e);
	                 try { Thread.sleep(20); } catch(InterruptedException interruptedException) { interruptedException.printStackTrace(); }
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

	@KafkaListener(topics = "seckill-orders", groupId = "seckill-order-group")
	public void consumeSeckillOrder(String message, Acknowledgment ack) {
	    try {
	        @SuppressWarnings("unchecked")
	        Map<String, Object> map = objectMapper.readValue(message, Map.class);
	        Long orderId = Long.valueOf(String.valueOf(map.get("id")));
	        Long userId = Long.valueOf(String.valueOf(map.get("userId")));
	        Long voucherId = Long.valueOf(String.valueOf(map.get("voucherId")));

	        VoucherOrder order = new VoucherOrder();
	        order.setId(orderId);
	        order.setUserId(userId);
	        order.setVoucherId(voucherId);

	        handleVoucherOrder(order);

	        if (ack != null) {
	            ack.acknowledge();
	        }
	    } catch (Exception e) {
	        log.error("消费秒杀订单消息失败: {}", message, e);
	        if (ack != null) {
	        	ack.nack(1000); // 暂停 1s 后重试
	        }
	    }
	}

	@Override
	@Transactional
	public Result buyVoucher(Long voucherId) {
		Long userId = UserHolder.getUser().getId();
		
		// 查询优惠券信息
		Voucher voucher = voucherService.getById(voucherId);
		if (voucher == null) {
			return Result.fail("优惠券不存在！");
		}
		
		// 检查优惠券类型，必须是普通券
		if (voucher.getType() == null || voucher.getType() != 0) {
			return Result.fail("该优惠券不是普通券，请使用秒杀接口！");
		}
		
		// 检查优惠券状态，必须是上架状态
		if (voucher.getStatus() == null || voucher.getStatus() != 1) {
			return Result.fail("优惠券未上架或已下架！");
		}
		
		// 创建订单（普通券允许用户多次购买，不需要一人一单限制）
		long orderId = redisIdWorker.nextId("order");
		VoucherOrder voucherOrder = new VoucherOrder();
		voucherOrder.setId(orderId);
		voucherOrder.setUserId(userId);
		voucherOrder.setVoucherId(voucherId);
		voucherOrder.setPayType(1); // 默认余额支付
		voucherOrder.setStatus(1); // 未支付状态
		voucherOrder.setCreateTime(LocalDateTime.now());
		
		// 保存订单（普通券不需要扣减库存）
		save(voucherOrder);
		
		// 生成支付二维码
		try {
			// 构建支付信息（包含订单ID、用户ID、优惠券ID、支付金额等）
			String payInfo = String.format("orderId=%d&userId=%d&voucherId=%d&amount=%d", 
					orderId, userId, voucherId, voucher.getPayValue());
			
			// 配置二维码参数
			QrConfig config = new QrConfig(300, 300);
			config.setMargin(1); // 设置边距
			
			// 生成二维码图片
			BufferedImage qrCodeImage = QrCodeUtil.generate(payInfo, config);
			
			// 将二维码图片转换为base64字符串
			ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
			ImageIO.write(qrCodeImage, "png", outputStream);
			byte[] qrCodeBytes = outputStream.toByteArray();
			String qrCodeBase64 = Base64.encode(qrCodeBytes);
			
			// 构建返回结果
			Map<String, Object> result = new HashMap<>();
			result.put("orderId", orderId);
			result.put("qrCode", "data:image/png;base64," + qrCodeBase64);
			result.put("payAmount", voucher.getPayValue());
			result.put("voucherTitle", voucher.getTitle());
			
			return Result.ok(result);
		} catch (IOException e) {
			log.error("生成支付二维码失败", e);
			// 即使二维码生成失败，也返回订单ID
			return Result.ok(orderId);
		}
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
		if (voucherOrder == null) {
			log.error("voucherOrder 为空，跳过处理");
			return;
		}
		// userId只能从voucherOrder取
		Long userId = voucherOrder.getUserId();
		if (userId == null) {
			log.error("订单缺少 userId，跳过处理，orderId={}, voucherId={}", voucherOrder.getId(), voucherOrder.getVoucherId());
			return;
		}
		// 确保代理在消费线程可用（某些实例可能未经过 seckillVoucher() 路径设置过 proxy）
		if (proxy == null) {
			try {
				proxy = applicationContext.getBean(IVoucherOrderService.class);
			} catch (Exception e) {
				log.error("获取 IVoucherOrderService 代理失败，跳过处理，orderId={}, userId={}", voucherOrder.getId(), userId, e);
				return;
			}
		}
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
