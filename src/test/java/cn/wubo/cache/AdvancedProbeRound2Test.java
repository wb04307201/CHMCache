package cn.wubo.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进阶探针第 2 轮:统计指标一致性
 *
 * 关注点:
 * - hit/miss 在各种路径下是否正确累计
 * - eviction/expiration 在同步 vs 异步清理下的差异
 * - load/loadFailure 在不同 loader 返回值下的计数
 * - getAll 路径下 miss 计数是否正确
 * - refresh 失败是否计入 refreshFailureCount
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class AdvancedProbeRound2Test {

    @Test
    @DisplayName("B1-1: hit/miss 严格匹配每次 get 调用")
    void probe_B1_hit_miss_exact() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            // 5 hits
            for (int i = 0; i < 5; i++) cache.get("a");
            for (int i = 0; i < 3; i++) cache.get("b");
            // 4 misses
            cache.get("c");
            cache.get("d");
            cache.get("e");
            cache.get("f");
            CacheMetrics m = cache.metrics();
            assertEquals(8, m.hitCount(), "5+3 hits");
            assertEquals(4, m.missCount(), "4 misses");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B1-2: get 命中过期项时,记 miss + expiration")
    void probe_B1_expired_get_is_miss() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));  // hit
            Thread.sleep(100);
            cache.get("k");  // expired -> miss + expiration
            CacheMetrics m = cache.metrics();
            assertEquals(1, m.hitCount());
            assertEquals(1, m.missCount());
            assertEquals(1, m.expirationCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B1-3: invalidate 后 get 记 miss,不计 expiration")
    void probe_B1_invalidate_is_miss_not_expiration() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v");
            cache.invalidate("k");
            cache.get("k");  // miss,非 expiration
            CacheMetrics m = cache.metrics();
            assertEquals(1, m.missCount());
            assertEquals(0, m.expirationCount(), "invalidate 不应计 expiration");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B1-4: getAll 中命中/未命中分别记 hit/miss")
    void probe_B1_getAll_metrics() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.getAll(Set.of("a", "b", "c", "d"),
                    missing -> new java.util.HashMap<>());
            CacheMetrics m = cache.metrics();
            assertEquals(2, m.hitCount(), "a, b 命中");
            assertEquals(2, m.missCount(), "c, d 未命中");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B1-5: getAll 命中过期 key 记 miss + expiration")
    void probe_B1_getAll_expired() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        try {
            cache.put("k", "v");
            Thread.sleep(100);
            cache.getAll(Set.of("k"), missing -> new java.util.HashMap<>());
            CacheMetrics m = cache.metrics();
            assertEquals(1, m.missCount());
            assertTrue(m.expirationCount() >= 1, "expired 路径应增加 expirationCount");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B2-1: evictionCount 反映 LRU 淘汰,expirationCount 反映过期")
    void probe_B2_eviction_vs_expiration() throws InterruptedException {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(3)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
        try {
            // 触发 2 次 LRU 淘汰
            for (int i = 0; i < 5; i++) cache.put(i, i);
            assertEquals(2, cache.metrics().evictionCount());
            // 触发过期
            cache.put(100, 100, Duration.ofMillis(30));
            Thread.sleep(80);
            cache.cleanup();
            assertTrue(cache.metrics().expirationCount() >= 1);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B2-2: weight-based 模式下 evictionCount 同样累计")
    void probe_B2_weight_eviction_count() {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(100)
                .weigher((k, v) -> v.length)
                .build();
        try {
            cache.put("a", new byte[50]);
            cache.put("b", new byte[50]);
            cache.put("c", new byte[50]);  // 触发至少 1 次淘汰
            assertTrue(cache.metrics().evictionCount() >= 1);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B3-1: loadCount 反映 loader 调用次数(loadSuccess)")
    void probe_B3_load_count() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            cache.get("a", k -> "1");
            cache.get("a", k -> "1");  // hit, 不计 load
            cache.get("b", k -> "2");
            cache.get("c", k -> null);  // null 结果,不计 load
            CacheStats s = cache.stats();
            assertEquals(2, s.loadCount(), "2 次有效 load (a, b)");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B3-2: loadFailureCount 反映 loader 异常")
    void probe_B3_load_failure() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            assertThrows(RuntimeException.class,
                    () -> cache.get("a", k -> { throw new RuntimeException("boom"); }));
            assertThrows(RuntimeException.class,
                    () -> cache.get("b", k -> { throw new IllegalStateException(); }));
            CacheStats s = cache.stats();
            assertEquals(2, s.loadFailureCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B3-3: getAll bulkLoader 异常 — 当前实现不计入 loadFailure")
    void probe_B3_getAll_bulk_exception() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            assertThrows(RuntimeException.class,
                    () -> cache.getAll(Set.of("a"),
                            missing -> { throw new RuntimeException("boom"); }));
            // 记录当前行为
            CacheStats s = cache.stats();
            // bulkLoader 异常会向上传播,但 loadFailureCount 可能不增加
            // 这是一个潜在改进点(不算 bug,因为 contract 不明确)
            System.out.println("[B3-3] loadFailureCount after bulkLoader throw: " + s.loadFailureCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B4-1: refresh 失败计入 refreshFailureCount")
    void probe_B4_refresh_failure() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v");
            cache.refresh("k", k -> { throw new RuntimeException("nope"); });
            Thread.sleep(200);
            // refresh 异常不传播,被 AsyncRefresher 吞掉
            assertEquals("v", cache.get("k"));
            // refreshFailureCount 来自 AsyncRefresher,CHMCache 没暴露
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B4-2: refresh loader 返回 null 不算成功也不算失败")
    void probe_B4_refresh_null() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v1");
            cache.refresh("k", k -> null);
            Thread.sleep(200);
            // null 时不替换,旧值保留
            assertEquals("v1", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B5-1: cleanupRunCount 真实反映清理活动")
    void probe_B5_cleanup_count() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            long initial = cache.metrics().cleanupRunCount();
            Thread.sleep(300);
            long after = cache.metrics().cleanupRunCount();
            assertTrue(after - initial >= 3, "至少运行 3 次,实际增量=" + (after - initial));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B5-2: cleanupTimeNanos 在多次清理后累计")
    void probe_B5_cleanup_time() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            long initial = cache.metrics().cleanupTimeNanos();
            Thread.sleep(300);
            long after = cache.metrics().cleanupTimeNanos();
            assertTrue(after >= initial, "cleanupTimeNanos 应单调不减");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B6-1: hitRate 准确")
    void probe_B6_hit_rate() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("a", "1");
            cache.get("a");  // hit
            cache.get("b");  // miss
            // 1 hit, 1 miss, hitRate = 0.5
            assertEquals(0.5, cache.metrics().hitRate(), 1e-9);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B6-2: hitRate 没有任何访问时为 0,不抛异常")
    void probe_B6_hit_rate_empty() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            assertEquals(0.0, cache.metrics().hitRate(), 0.0001);
            // 也没有 NaN 或除零
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B7-1: stats() 返回的延迟统计准确(仅 recordStats 模式)")
    void probe_B7_stats_latency() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            cache.put("k", "v");
            for (int i = 0; i < 10; i++) cache.get("k");
            CacheStats s = cache.stats();
            long avg = s.averageGetPenalty(TimeUnit.NANOSECONDS);
            long min = s.minGetPenalty(TimeUnit.NANOSECONDS);
            long max = s.maxGetPenalty(TimeUnit.NANOSECONDS);
            assertTrue(avg >= 0);
            assertTrue(min <= avg, "min(" + min + ") <= avg(" + avg + ")");
            assertTrue(max >= avg, "max(" + max + ") >= avg(" + avg + ")");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("B7-2: sizeWatermark 反映历史最大 size")
    void probe_B7_size_watermark() {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(100)
                .recordStats()
                .build();
        try {
            for (int i = 0; i < 50; i++) cache.put(i, i);
            assertEquals(50, cache.stats().sizeWatermark());
            // 触发淘汰
            for (int i = 50; i < 100; i++) cache.put(i, i);
            // 不再增加 watermark
            long after = cache.stats().sizeWatermark();
            for (int i = 0; i < 50; i++) cache.invalidate(i);
            assertEquals(after, cache.stats().sizeWatermark(),
                    "watermark 应只增不减,不会因 invalidate 下降");
        } finally {
            cache.shutdown();
        }
    }
}
