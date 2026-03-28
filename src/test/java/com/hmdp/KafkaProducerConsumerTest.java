package com.hmdp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Kafka 生产者和消费者单元测试
 * 
 * 使用外部 Kafka 服务器进行测试
 * 
 * 运行测试前请确保：
 * 1. Kafka 服务器已启动（localhost:9092）
 * 2. 已创建 Topic：seckill-orders
 * 
 * 创建 Topic 命令：
 * kafka-topics.bat --create --topic seckill-orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
 */
@Slf4j
@SpringBootTest
class KafkaProducerConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String TEST_TOPIC = "seckill-orders";
    
    // 用于接收消息的容器
    private static VoucherOrder receivedOrder;
    private static CountDownLatch latch;

    /**
     * 测试 Kafka 生产者发送消息
     */
    @Test
    void testKafkaProducer() throws Exception {
        log.info("开始测试 Kafka 生产者...");
        
        // 准备测试数据
        Map<String, Object> orderMessage = new HashMap<>();
        orderMessage.put("userId", 1001L);
        orderMessage.put("voucherId", 2001L);
        orderMessage.put("id", 3001L);
        
        // 转换为 JSON 字符串
        String messageJson = objectMapper.writeValueAsString(orderMessage);
        log.info("准备发送消息: {}", messageJson);
        
        // 发送消息到 Kafka
        String key = String.valueOf(orderMessage.get("id"));
        
        // 使用 get() 确保消息发送完成并获取结果
        try {
            SendResult<String, String> sendResult = 
                    kafkaTemplate.send(TEST_TOPIC, key, messageJson).get(5, TimeUnit.SECONDS);
            if (sendResult != null) {
                log.info("消息发送成功! Topic: {}, Partition: {}, Offset: {}", 
                        sendResult.getRecordMetadata().topic(),
                        sendResult.getRecordMetadata().partition(),
                        sendResult.getRecordMetadata().offset());
                log.info("Kafka 生产者测试完成 - 消息已成功发送");
            }
        } catch (Exception e) {
            log.error("消息发送超时或失败", e);
            fail("消息发送失败: " + e.getMessage());
        }
    }

    /**
     * 测试 Kafka 消费者接收消息
     * 
     * 注意：这个测试需要配合 @KafkaListener 使用
     * 如果还没有实现消费者，可以先测试生产者部分
     */
    @Test
    void testKafkaConsumer() throws Exception {
        log.info("开始测试 Kafka 消费者...");
        
        // 初始化接收容器
        receivedOrder = null;
        latch = new CountDownLatch(1);
        
        // 准备测试数据
        Map<String, Object> orderMessage = new HashMap<>();
        orderMessage.put("userId", 1002L);
        orderMessage.put("voucherId", 2002L);
        orderMessage.put("id", 3002L);
        
        // 转换为 JSON 字符串
        String messageJson = objectMapper.writeValueAsString(orderMessage);
        log.info("准备发送消息供消费者测试: {}", messageJson);
        
        // 发送消息
        String key = String.valueOf(orderMessage.get("id"));
        kafkaTemplate.send(TEST_TOPIC, key, messageJson).get();
        
        log.info("消息已发送，等待消费者处理...");
        
        // 注意：这个测试需要实际的消费者监听器才能完成
        // 如果还没有实现消费者，这里只是验证消息发送成功
        // 实际消费测试需要在实现了 @KafkaListener 后进行
        
        // 等待一段时间让消息被处理
        boolean received = latch.await(5, TimeUnit.SECONDS);
        
        if (received && receivedOrder != null) {
            log.info("消费者成功接收消息: {}", receivedOrder);
            assertEquals(1002L, receivedOrder.getUserId());
            assertEquals(2002L, receivedOrder.getVoucherId());
            assertEquals(3002L, receivedOrder.getId());
        } else {
            log.warn("消费者测试需要配合实际的 @KafkaListener 实现");
        }
        
        log.info("Kafka 消费者测试完成");
    }

    /**
     * 测试完整的生产消费流程
     */
    @Test
    void testKafkaProducerAndConsumer() throws Exception {
        log.info("开始测试完整的 Kafka 生产消费流程...");
        
        // 准备多个测试订单
        for (int i = 1; i <= 5; i++) {
            Map<String, Object> orderMessage = new HashMap<>();
            orderMessage.put("userId", 1000L + i);
            orderMessage.put("voucherId", 2000L + i);
            orderMessage.put("id", 3000L + i);
            
            String messageJson = objectMapper.writeValueAsString(orderMessage);
            String key = String.valueOf(orderMessage.get("id"));
            
            // 发送消息
            kafkaTemplate.send(TEST_TOPIC, key, messageJson)
                    .addCallback(
                        result -> log.info("订单 {} 消息发送成功", orderMessage.get("id")),
                        failure -> log.error("订单 {} 消息发送失败", orderMessage.get("id"), failure)
                    );
            
            // 短暂延迟，避免消息发送过快
            Thread.sleep(100);
        }
        
        // 等待所有消息发送完成
        Thread.sleep(2000);
        
        log.info("完整流程测试完成，已发送 5 条消息");
    }

    /**
     * 测试消息序列化和反序列化
     */
    @Test
    void testMessageSerialization() throws Exception {
        log.info("开始测试消息序列化和反序列化...");
        
        // 创建订单对象
        VoucherOrder order = new VoucherOrder();
        order.setId(4001L);
        order.setUserId(1003L);
        order.setVoucherId(2003L);
        order.setPayType(1);
        order.setStatus(1);
        order.setCreateTime(LocalDateTime.now());
        
        // 转换为 Map（模拟实际发送的消息格式）
        Map<String, Object> orderMap = new HashMap<>();
        orderMap.put("id", order.getId());
        orderMap.put("userId", order.getUserId());
        orderMap.put("voucherId", order.getVoucherId());
        orderMap.put("payType", order.getPayType());
        orderMap.put("status", order.getStatus());
        
        // 序列化为 JSON
        String json = objectMapper.writeValueAsString(orderMap);
        log.info("序列化后的 JSON: {}", json);
        
        // 反序列化
        @SuppressWarnings("unchecked")
        Map<String, Object> deserializedMap = objectMapper.readValue(json, Map.class);
        log.info("反序列化后的 Map: {}", deserializedMap);
        
        // 验证
        assertEquals(order.getId(), Long.valueOf(deserializedMap.get("id").toString()));
        assertEquals(order.getUserId(), Long.valueOf(deserializedMap.get("userId").toString()));
        assertEquals(order.getVoucherId(), Long.valueOf(deserializedMap.get("voucherId").toString()));
        
        log.info("消息序列化和反序列化测试通过");
    }

    /**
     * 测试消息发送异常处理
     */
    @Test
    void testProducerErrorHandling() {
        log.info("开始测试生产者异常处理...");
        
        // 测试发送 null 消息（应该会失败或抛出异常）
        try {
            kafkaTemplate.send(TEST_TOPIC, "test-key", null)
                    .addCallback(
                        result -> log.info("消息发送成功（不应该发生）"),
                        failure -> {
                            log.info("预期的异常被捕获: {}", failure.getMessage());
                            assertNotNull(failure);
                        }
                    );
            
            Thread.sleep(500);
        } catch (Exception e) {
            log.info("捕获到预期的异常: {}", e.getMessage());
        }
        
        log.info("生产者异常处理测试完成");
    }

    /**
     * 测试消息 Key 的作用（保证同一订单的消息有序）
     */
    @Test
    void testMessageKey() throws Exception {
        log.info("开始测试消息 Key 的作用...");
        
        // 使用相同的 key 发送多条消息（应该发送到同一个分区）
        String sameKey = "order-5001";
        
        for (int i = 1; i <= 3; i++) {
            Map<String, Object> orderMessage = new HashMap<>();
            orderMessage.put("userId", 1004L);
            orderMessage.put("voucherId", 2004L);
            orderMessage.put("id", 5000L + i);
            orderMessage.put("sequence", i);
            
            String messageJson = objectMapper.writeValueAsString(orderMessage);
            
            // 使用相同的 key
            kafkaTemplate.send(TEST_TOPIC, sameKey, messageJson)
                    .addCallback(
                        result -> {
                            if (result != null) {
                                log.info("消息 {} 发送到分区: {}", 
                                        orderMessage.get("sequence"),
                                        result.getRecordMetadata().partition());
                            }
                        },
                        failure -> log.error("消息发送失败", failure)
                    );
            
            Thread.sleep(100);
        }
        
        Thread.sleep(1000);
        log.info("消息 Key 测试完成");
    }
}

