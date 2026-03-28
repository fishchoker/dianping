package com.hmdp.service.impl;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

/**
 * 将 Redis Outbox 中的消息转发到 Kafka
 * Outbox 列表：queue:seckill:pending
 * 消息格式：orderId:userId:voucherId
 */
@Slf4j
@Component
public class OutboxForwarder {

	private static final String OUTBOX_KEY = "queue:seckill:pending";
	private static final String KAFKA_TOPIC = "seckill-orders";

	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private volatile boolean running = true;

	@Resource
	private StringRedisTemplate stringRedisTemplate;
	@Resource
	private KafkaTemplate<String, String> kafkaTemplate;
	@Resource
	private ObjectMapper objectMapper;

	@PostConstruct
	public void start() {
		executor.submit(this::runLoop);
	}

	@PreDestroy
	public void shutdown() {
		running = false;
		executor.shutdownNow();
	}

	private void runLoop() {
		log.info("OutboxForwarder started");
		while (running) {
			try {
				// 使用 BRPOP 阻塞读取（超时2秒，便于优雅退出）
				String msg = stringRedisTemplate.opsForList().leftPop(OUTBOX_KEY, Duration.ofSeconds(2));
				if (msg == null) {
					continue;
				}
				// 解析消息：orderId:userId:voucherId
				String[] parts = msg.split(":");
				if (parts.length != 3) {
					log.warn("Outbox message format invalid: {}", msg);
					continue;
				}
				long orderId = Long.parseLong(parts[0]);
				long userId = Long.parseLong(parts[1]);
				long voucherId = Long.parseLong(parts[2]);

				Map<String, Object> payload = new HashMap<>();
				payload.put("id", orderId);
				payload.put("userId", userId);
				payload.put("voucherId", voucherId);

				String json = objectMapper.writeValueAsString(payload);

				// 发送到 Kafka，使用订单ID作为 key，确保同一订单有序
				kafkaTemplate.send(KAFKA_TOPIC, String.valueOf(orderId), json).addCallback(
						result -> {
							if (result != null) {
								log.info("Outbox->Kafka success. orderId={}, partition={}, offset={}", orderId,
										result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
							}
						},
						ex -> {
							log.error("Outbox->Kafka failed. orderId={}, msg will be re-queued", orderId, ex);
							// 发送失败，放回队列尾部，稍后重试
							try {
								stringRedisTemplate.opsForList().rightPush(OUTBOX_KEY, msg);
							} catch (Exception pushEx) {
								log.error("Re-queue outbox message failed: {}", msg, pushEx);
							}
						});
			} catch (Exception e) {
				log.error("OutboxForwarder loop error", e);
			}
		}
		log.info("OutboxForwarder stopped");
	}
}


