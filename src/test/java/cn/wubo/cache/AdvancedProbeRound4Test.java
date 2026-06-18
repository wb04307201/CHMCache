package cn.wubo.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进阶探针第 4 轮:聚焦 weight 一致性与新发现的边界问题
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class AdvancedProbeRound4Test {

    @Test
    @DisplayName("D1-1: weight-based 模式,put + 后台过期后,weightedSize 应正确")
    void probe_D1_weight_after_expiration() throws InterruptedException {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(1000)
                .weigher((k, v) -> v.length)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            cache.put("a", new byte[50]);
            cache.put("b", new byte[50]);
            assertEquals(100, cache.weightedSize());
            // 等过期 + 后台清理
            Thread.sleep(300);
            // weightedSize 应归 0
            assertEquals(0, cache.weightedSize(),
                    "expire 后 weightedSize 应正确归零,实际=" + cache.weightedSize());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("D1-2: 反复 put + 过期 + 清理,currentWeight 不应单调递增")
    void probe_D1_weight_no_drift() throws InterruptedException {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(1000)
                .weigher((k, v) -> v.length)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            for (int round = 0; round < 5; round++) {
                for (int i = 0; i < 10; i++) {
                    cache.put("k" + i, new byte[10]);
                }
                assertEquals(100, cache.weightedSize());
                Thread.sleep(200);  // 等过期 + 清理
                assertEquals(0, cache.weightedSize(),
                        "round " + round + " 清理后 weightedSize 应为 0,实际=" + cache.weightedSize());
            }
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("D1-3: 大量过期后,weightedSize 不应继续增长超过 maxWeight")
    void probe_D1_weight_does_not_explode() throws InterruptedException {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(500)
                .weigher((k, v) -> v.length)
                .expireAfterWrite(Duration.ofMillis(30))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            // 短时间大量 put + 过期
            for (int i = 0; i < 100; i++) {
                cache.put("k" + i, new byte[10], Duration.ofMillis(30));
            }
            Thread.sleep(500);
            // weightedSize 应回落到 0 或很小(若新的没过期)
            long ws = cache.weightedSize();
            assertTrue(ws <= 500, "weightedSize 超过 maxWeight,实际=" + ws);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("D1-4: 手动 cleanup() 也应正确减少 weightedSize")
    void probe_D1_weight_after_manual_cleanup() throws InterruptedException {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(1000)
                .weigher((k, v) -> v.length)
                .cleanupInterval(Duration.ofSeconds(60))  // 关闭后台
                .build();
        try {
            cache.put("a", new byte[50], Duration.ofMillis(50));
            cache.put("b", new byte[50], Duration.ofMillis(50));
            assertEquals(100, cache.weightedSize());
            Thread.sleep(150);
            cache.cleanup();
            assertEquals(0, cache.weightedSize(),
                    "手动 cleanup 后 weightedSize 应为 0,实际=" + cache.weightedSize());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("D2-1: weightedSize 与 cacheMap.size() 同步变化")
    void probe_D2_weight_size_consistency() {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(10000)
                .weigher((k, v) -> v.length)
                .build();
        try {
            for (int i = 0; i < 10; i++) cache.put("k" + i, new byte[10]);
            assertEquals(10, cache.size());
            assertEquals(100, cache.weightedSize());
            // 逐个 invalidate
            for (int i = 0; i < 10; i++) {
                cache.invalidate("k" + i);
                assertEquals(9 - i, cache.size());
                assertEquals(10 * (9 - i), cache.weightedSize());
            }
        } finally {
            cache.shutdown();
        }
    }
}
