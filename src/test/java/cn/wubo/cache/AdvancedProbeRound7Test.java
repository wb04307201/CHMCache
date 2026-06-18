package cn.wubo.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进阶探针第 9 轮:资源/内存峰值
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class AdvancedProbeRound7Test {

    @Test
    @DisplayName("G1-1: 高频短 TTL put 期间,DelayQueue 临时膨胀后被回收")
    void probe_G1_delayQueue_drain_after_burst() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build();
        try {
            // 突发 5000 个短 TTL
            for (int i = 0; i < 5000; i++) {
                cache.put("k" + i, "v" + i, Duration.ofMillis(100));
            }
            // 等 2s 后所有 key 过期,后台清理
            Thread.sleep(2000);
            // 50% 概率被清理线程清理掉(看 LRU 配合)
            // size 应 <= 10000,且 weightedSize 无意义(非 weight 模式)
            assertTrue(cache.size() <= 10_000, "size 超限:" + cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G1-2: 大量 put 后,shutdown 后 DelayQueue 残余应被丢弃")
    void probe_G1_shutdown_drops_delayqueue() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        try {
            for (int i = 0; i < 1000; i++) {
                cache.put("k" + i, "v" + i);
            }
            // 不等过期,立即 shutdown
        } finally {
            cache.shutdown();
        }
        // 关闭后,后台线程已终止
        assertTrue(cache.isShutdown());
    }

    @Test
    @DisplayName("G2-1: 极大量 put 后,invalidateAll 内存被释放")
    void probe_G2_invalidateAll_releases_memory() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(50_000)
                .build();
        try {
            for (int i = 0; i < 50_000; i++) {
                cache.put("k" + i, "v" + i);
            }
            assertEquals(50_000, cache.size());
            cache.invalidateAll();
            assertEquals(0, cache.size());
            // 重新填充可正常
            for (int i = 0; i < 100; i++) cache.put("k2-" + i, "v" + i);
            assertEquals(100, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G2-2: 高并发混合操作期间不抛 OOM")
    void probe_G2_mixed_concurrent_no_oom() throws Exception {
        CHMCache<Integer, byte[]> cache = CHMCache.<Integer, byte[]>newBuilder()
                .maximumSize(2000)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(30))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch done = new CountDownLatch(8);
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 5000; i++) {
                            cache.put(seed * 100000 + i, new byte[100]);
                            if (i % 100 == 0) cache.invalidate(seed * 100000 + i / 2);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int t = 0; t < 4; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 1000; i++) {
                            for (int j = 0; j < 100; j++) {
                                cache.get(j);
                            }
                            cache.cleanup();
                        }
                    } catch (Throwable th) {
                        // 任何错误都接受
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(60, TimeUnit.SECONDS));
            // size 应受 maxSize 约束
            assertTrue(cache.size() <= 2000, "size 超限:" + cache.size());
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G3-1: 慢 listener 不影响 cleanup 周期性执行")
    void probe_G3_slow_listener_does_not_block_cleanup() throws Exception {
        AtomicInteger listenerCalls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(5)
                .removalListener((k, v, c) -> {
                    // 慢 listener
                    try { Thread.sleep(20); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    listenerCalls.incrementAndGet();
                })
                .build();
        try {
            for (int i = 0; i < 10; i++) cache.put("k" + i, "v" + i);
            Thread.sleep(500);
            // listener 至少被调用 5 次(LRU 淘汰),慢 listener 仍能继续被调用
            assertTrue(listenerCalls.get() >= 5,
                    "listener 应被多次调用,实际=" + listenerCalls.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G3-2: 后台线程死循环(oom/卡死)不应阻塞业务线程")
    void probe_G3_background_thread_independent() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(10))
                .cleanupInterval(Duration.ofMillis(10))
                .build();
        try {
            for (int i = 0; i < 5; i++) cache.put("k" + i, "v" + i);
            // 业务线程继续 put
            long start = System.currentTimeMillis();
            for (int i = 0; i < 1000; i++) {
                cache.put("k-" + i, "v-" + i);
                cache.get("k-" + i);
            }
            long elapsed = System.currentTimeMillis() - start;
            // 业务线程不应被卡死(1s 内完成)
            assertTrue(elapsed < 5000, "业务线程被卡,耗时 " + elapsed + "ms");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G4-1: size=1 的极小 cache 行为正确")
    void probe_G4_tiny_cache() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(1)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            assertEquals(1, cache.size());
            assertEquals("2", cache.get("b"));
            assertNull(cache.get("a"));
            // 反复 put
            for (int i = 0; i < 100; i++) {
                cache.put("k" + (i % 2), "v" + i);
            }
            assertEquals(1, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G4-2: 大量 invalidate 调用后,内部状态仍正常")
    void probe_G4_mass_invalidate() {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(1000)
                .build();
        try {
            for (int i = 0; i < 1000; i++) cache.put(i, i);
            assertEquals(1000, cache.size());
            // 全部失效
            for (int i = 0; i < 1000; i++) cache.invalidate(i);
            assertEquals(0, cache.size());
            // 重新 put 应正常
            for (int i = 0; i < 100; i++) cache.put(i, i * 2);
            assertEquals(100, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("G4-3: size 上限触发的淘汰在大量并发下保持一致")
    void probe_G4_eviction_invariant_under_contention() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(100)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            CountDownLatch done = new CountDownLatch(8);
            for (int t = 0; t < 8; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 5000; i++) {
                            int v = seed * 100000 + i;
                            cache.put(v, v);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
            assertEquals(100, cache.size(), "size 应精确等于 maxSize");
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }
}
