package com.hmdp.config;

import java.util.HashSet;
import java.util.Set;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * 迁移历史错误的关注集合键名：从 "follows: <uid>"（多了空格）迁移到 "follows:<uid>"
 * 使用 SCAN 渐进式扫描，避免阻塞。
 */
@Component
@Order(1)
public class FollowsKeyMigrationRunner implements ApplicationRunner {
	private static final Logger log = LoggerFactory.getLogger(FollowsKeyMigrationRunner.class);
	private static final String OLD_PREFIX = "follows: ";
	private static final String NEW_PREFIX = "follows:";
	
	@Resource
	private StringRedisTemplate stringRedisTemplate;
	
	@Override
	public void run(ApplicationArguments args) throws Exception {
		// 扫描匹配旧前缀的 key
		ScanOptions options = ScanOptions.scanOptions().match(OLD_PREFIX + "*").count(500).build();
		try (Cursor<byte[]> cursor = stringRedisTemplate.getConnectionFactory()
				.getConnection()
				.scan(options)) {
			Set<String> migrated = new HashSet<>();
			while (cursor.hasNext()) {
				String oldKey = new String(cursor.next());
				// 计算新 key
				String newKey = NEW_PREFIX + oldKey.substring(OLD_PREFIX.length());
				if (migrated.contains(oldKey)) {
					continue;
				}
				try {
					// 读取旧集合成员
					Set<String> members = stringRedisTemplate.opsForSet().members(oldKey);
					if (members != null && !members.isEmpty()) {
						// 写入新集合
						stringRedisTemplate.opsForSet().add(newKey, members.toArray(new String[0]));
					}
					// 删除旧 key
					stringRedisTemplate.delete(oldKey);
					migrated.add(oldKey);
					log.info("Migrated follows key from '{}' to '{}', members={}", oldKey, newKey, (members == null ? 0 : members.size()));
				} catch (Exception ex) {
					log.error("Migrate follows key failed, oldKey={}", oldKey, ex);
				}
			}
		} catch (Exception e) {
			log.error("Scan follows keys for migration failed", e);
		}
	}
}

