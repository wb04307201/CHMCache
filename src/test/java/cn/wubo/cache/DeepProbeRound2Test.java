package cn.wubo.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 第二轮深度探针:在已知 Bug 1 (removalListener null) 基础上,
 * 继续探测 token 安全性、统计指标一致性、AccessOrderTracker 与 cacheMap 大小一致性等
 * 隐性问题。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class DeepProbeRound2Test {

    /**
     * Bug 探测 A: accessOrder 与 cacheMap 大小一致性
     * 在 put + invalidate 反复操作后,内部状态是否一致?
     */
    @Test
    @DisplayName("Probe A: 反复 put/invalidate 后内部状态一致")
    void probe_A_state_consistency() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .build();
        try {
            // 反复 put + invalidate,模拟正常使用
            for (int round = 0; round < 50; round++) {
                for (int i = 0; i < 50; i++) {
                    cache.put("k" + i, "v" + round);
                }
                for (int i = 0; i < 50; i++) {
                    cache.invalidate("k" + i);
                }
            }
            assertEquals(0, cache.size());
            // 重新填充应正常
            cache.put("fresh", "v");
            assertEquals(1, cache.size());
            assertEquals("v", cache.get("fresh"));
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 B: invalidateAll 后,AccessOrderTracker 残留项检查
     */
    @Test
    @DisplayName("Probe B: invalidateAll 后 accessOrder 应被清空")
    void probe_B_invalidateAll_clears_access_order() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .build();
        try {
            for (int i = 0; i < 20; i++) cache.put("k" + i, "v");
            cache.invalidateAll();
            assertEquals(0, cache.size());
            // 重新 put 100 个,不应因残留 accessOrder 触发意外淘汰
            for (int i = 0; i < 100; i++) cache.put("k" + i, "v");
            assertEquals(100, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 C: getAll + 触发 LRU 淘汰
     * getAll 中通过 put 回填时,put 会触发 evictIfNeeded。
     * 这是否会在并发场景下出问题?
     */
    @Test
    @DisplayName("Probe C: getAll 回填触发 LRU 淘汰,不应丢失结果")
    void probe_C_getAll_triggers_eviction() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            // 先填满到 maxSize
            for (int i = 0; i < 10; i++) cache.put("k" + i, "v" + i);
            // getAll 触发 5 个 put,会淘汰 5 个
            java.util.Map<String, String> r = cache.getAll(
                    Set.of("a", "b", "c", "d", "e"),
                    missing -> {
                        java.util.HashMap<String, String> m = new java.util.HashMap<>();
                        for (String k : missing) m.put(k, "loaded-" + k);
                        return m;
                    });
            assertEquals(5, r.size());
            assertTrue(cache.size() <= 10);
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 D: token 序列溢出
     * tokenGenerator 是 AtomicLong,理论上限为 Long.MAX_VALUE。
     * 在高频 put 场景下,多久会用完?正常应用不会触及。
     * 仅作为记录。
     */

    /**
     * Bug 探测 E: cleanupExecutor 在 shutdown 后被多次调用
     */
    @Test
    @DisplayName("Probe E: shutdown 后 cleanup() 不抛异常")
    void probe_E_cleanup_after_shutdown() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        cache.shutdown();
        try {
            // shutdown 后调 cleanup 不应抛异常
            cache.put("k", "v");
            cache.cleanup();  // 不应抛
            Thread.sleep(100);
            cache.cleanup();  // 也不应抛
            assertNull(cache.get("k"));  // 已过期
        } finally {
            // 二次 shutdown 也不应抛
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 F: 在 LRU 淘汰过程中是否可能 evict 不应淘汰的 key?
     * (例如:被 invalidate 后又被 put,token 流转)
     */
    @Test
    @DisplayName("Probe F: 反复 put/invalidate 同一 key,size 不会膨胀")
    void probe_F_no_size_inflation() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(50)
                .build();
        try {
            for (int i = 0; i < 5000; i++) {
                cache.put("k", "v" + i);
                if (i % 3 == 0) cache.invalidate("k");
            }
            // size 不应超过 maxSize
            assertTrue(cache.size() <= 50, "size=" + cache.size());
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 G: computeIfAbsent + sliding TTL
     * computeIfAbsent 内部走 get(key, loader)。当 slidingTtl=true 时,
     * loader 加载完成后 put,put 也会触发 sliding 续期吗?
     */
    @Test
    @DisplayName("Probe G: computeIfAbsent 与 sliding TTL 交互")
    void probe_G_computeIfAbsent_slidingTtl() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterAccess(Duration.ofMillis(500))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            // loader 加载完成后写入,后续 get 应能续期
            String v = cache.computeIfAbsent("k", k -> "loaded");
            assertEquals("loaded", v);
            Thread.sleep(200);
            // 此时距 put 200ms,get 应续期(TTL=500ms)
            String v2 = cache.get("k");
            assertEquals("loaded", v2);
            Thread.sleep(200);
            // 距上次 get 200ms,get 应再续期
            String v3 = cache.get("k");
            assertEquals("loaded", v3);
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 H: expireAfterRead 失效?
     * 自定义 Expiry 的 expireAfterRead 在 get 命中时被调用,
     * 确认 sliding 真的工作。
     */
    @Test
    @DisplayName("Probe H: 自定义 Expiry 的 expireAfterRead 是否被调用")
    void probe_H_expiry_expireAfterRead_called() {
        AtomicInteger createCalls = new AtomicInteger();
        AtomicInteger readCalls = new AtomicInteger();
        Expiry<String, String> expiry = new Expiry<>() {
            @Override
            public Duration expireAfterCreate(String k, String v, long t) {
                createCalls.incrementAndGet();
                return Duration.ofMinutes(5);
            }
            @Override
            public Duration expireAfterRead(String k, String v, long t) {
                readCalls.incrementAndGet();
                return Duration.ofMinutes(5);
            }
        };
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter(expiry)
                .build();
        try {
            cache.put("k", "v");
            cache.get("k");
            cache.get("k");
            cache.get("k");
            assertEquals(1, createCalls.get());
            assertEquals(3, readCalls.get());
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 I: 在 put 路径触发 LRU 淘汰时,是否调用 removalListener
     */
    @Test
    @DisplayName("Probe I: LRU 淘汰时 removalListener 收到正确 cause")
    void probe_I_lru_eviction_listener_cause() {
        List<RemovalCause> causes = new ArrayList<>();
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2)
                .removalListener((k, v, c) -> causes.add(c))
                .build();
        try {
            cache.put(1, 1);
            cache.put(2, 2);
            cache.put(3, 3);  // 应淘汰 1
            cache.put(4, 4);  // 应淘汰 2
            assertEquals(2, causes.size());
            // 两次淘汰都应是 SIZE(在 size-based 模式下)
            for (RemovalCause c : causes) {
                assertEquals(RemovalCause.SIZE, c, "eviction cause should be SIZE, got: " + c);
            }
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 J: invalidateIf 在 put 期间并发执行
     */
    @Test
    @DisplayName("Probe J: 高并发 invalidateIf + put 不死锁")
    void probe_J_concurrent_invalidateIf() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2000)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            // 预填
            for (int i = 0; i < 1000; i++) cache.put(i, i);
            CountDownLatch done = new CountDownLatch(8);
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 500; i++) {
                            cache.put(seed * 10000 + i, i);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int round = 0; round < 20; round++) {
                            // 谓词匹配奇数 key
                            cache.invalidateIf(k -> ((Integer) k) % 2 == seed % 2);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS), "deadlock?");
            assertTrue(cache.size() <= 2000);
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 K: 同一 key 的 put 在 slidingTtl 模式下的 accessTimeNanos
     * CacheValue 中 accessTimeNanos 在 sliding TTL get 时应该被刷新。
     * 现有实现:refreshSlidingTtl 替换为新 CacheValue,新 CacheValue 构造时
     * accessTimeNanos = createTimeNanos(等于 new CacheValue 的构造时刻)。
     * 这是正确的(等效于刷新)。
     */
    @Test
    @DisplayName("Probe K: sliding TTL 续期后 accessTime 真的更新")
    void probe_K_sliding_access_time_updated() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterAccess(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            cache.put("k", "v");
            // 等待接近过期
            Thread.sleep(80);
            // get 触发续期
            assertEquals("v", cache.get("k"));
            // 再等 80ms,应仍在
            Thread.sleep(80);
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 L: 极小 maxSize (1) 是否行为正确
     */
    @Test
    @DisplayName("Probe L: maximumSize=1 行为")
    void probe_L_size_one() {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(1)
                .build();
        try {
            cache.put(1, 1);
            cache.put(2, 2);
            assertEquals(1, cache.size());
            assertEquals(2, cache.get(2));
            assertNull(cache.get(1));
        } finally {
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 M: 大量并发 put 同一 key + 短 TTL,get 仍能正确读到值
     */
    @Test
    @DisplayName("Probe M: 并发 put 同一 key 短 TTL,get 不抛异常")
    void probe_M_concurrent_short_ttl_put() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            int threads = 4;
            int writes = 1000;
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger errors = new AtomicInteger();
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                futures.add(pool.submit(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < writes; i++) {
                            cache.put("k", "v" + seed + "-" + i, Duration.ofMillis(20));
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }));
            }
            for (Future<?> f : futures) f.get(30, TimeUnit.SECONDS);
            assertEquals(0, errors.get());
            // 读
            for (int i = 0; i < 100; i++) cache.get("k");
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 N: 在 cleanup 期间 cacheMap 大小变化是否导致 CME
     */
    @Test
    @DisplayName("Probe N: cleanup 期间并发 put,无 CME/NPE")
    void probe_N_cleanup_concurrent_put() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch done = new CountDownLatch(4);
            for (int t = 0; t < 2; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 2000; i++) {
                            cache.put("k" + ((seed * 7 + i) % 200), "v");
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 100; i++) {
                            cache.cleanup();
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        // 不应抛
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    /**
     * Bug 探测 O: refresh 加载完后 put 的新值是否影响原有 LRU 顺序
     */
    @Test
    @DisplayName("Probe O: refresh 后的 put 触发的 eviction 行为正确")
    void probe_O_refresh_lru_order() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(3)
                .shardedLocks(false)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");
            // 此时 LRU 顺序(最旧 -> 最新):a, b, c
            // 模拟 refresh: 异步 put a 新值
            cache.refresh("a", k -> "1-refreshed");
            // 等待异步完成
            Thread.sleep(200);
            // a 被替换,accessOrder.touch 移到队尾,LRU 顺序:b, c, a
            // put d 触发淘汰,应淘汰 b(最旧)
            cache.put("d", "4");
            assertEquals("1-refreshed", cache.get("a"));
            assertNull(cache.get("b"), "b 应被淘汰(最旧)");
            assertEquals("3", cache.get("c"));
            assertEquals("4", cache.get("d"));
        } finally {
            cache.shutdown();
        }
    }
}
