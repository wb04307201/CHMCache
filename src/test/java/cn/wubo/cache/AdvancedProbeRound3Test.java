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
 * 进阶探针第 3 轮:线程安全/弱一致性/谓词副作用
 *
 * 关注点:
 * - ConcurrentHashMap 的弱一致迭代器在 put/invalidate 并发时是否丢数据
 * - invalidateIf 谓词在迭代中修改 cache 的副作用
 * - getAll + concurrent invalidate 的一致性
 * - cleanup 期间的弱一致性
 * - metrics snapshot 在并发下的准确性
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class AdvancedProbeRound3Test {

    @Test
    @DisplayName("C1-1: 并发 invalidateIf 不会丢失应失效的 key")
    void probe_C1_invalidateIf_concurrent() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2000)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            // 预填
            for (int i = 0; i < 1000; i++) cache.put(i, i);
            CountDownLatch done = new CountDownLatch(4);
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        // 每个线程失效不同范围的 key
                        cache.invalidateIf(k -> ((Integer) k) % 4 == seed);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(10, TimeUnit.SECONDS));
            // 4 个线程联合覆盖了 1000 个 key 的所有 %4 类别
            // 但 invalidateIf 串行执行,可能一个线程跑完后,其他线程看到的 key 已不在
            // 所以最终应该全部失效
            for (int i = 0; i < 1000; i++) {
                assertNull(cache.get(i), "key " + i + " 应已失效");
            }
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C1-2: 谓词在 invalidateIf 中对 cache 自身有副作用不应破坏")
    void probe_C1_predicate_side_effects() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");
            // 谓词中 put 新 key(理论上有副作用风险)
            cache.invalidateIf(k -> {
                if (k.equals("a")) {
                    cache.put("side-effect", "x");
                    return true;
                }
                return false;
            });
            // 副作用已发生
            assertEquals("x", cache.get("side-effect"));
            // a 应被失效
            assertNull(cache.get("a"));
            // b/c 应仍在
            assertEquals("2", cache.get("b"));
            assertEquals("3", cache.get("c"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C1-3: invalidateIf 谓词抛异常时,部分 key 可能已失效,部分未失效")
    void probe_C1_predicate_throws() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");
            // 谓词在 "b" 时抛异常
            assertThrows(RuntimeException.class, () -> cache.invalidateIf(k -> {
                if (k.equals("b")) throw new RuntimeException("boom");
                return k.equals("a");
            }));
            // a 应已被失效
            assertNull(cache.get("a"));
            // b/c 状态不确定(取决于迭代顺序)
            // 这是行为记录,不算 bug
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C2-1: 弱一致迭代器在并发 put 时不抛 CME")
    void probe_C2_weak_iterator() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2000)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            // 预填
            for (int i = 0; i < 500; i++) cache.put(i, i);
            CountDownLatch done = new CountDownLatch(4);
            // 2 个线程 put
            for (int t = 0; t < 2; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 2000; i++) {
                            cache.put(seed * 10000 + i, i);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            // 2 个线程 invalidateAll 反复
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 100; i++) {
                            cache.invalidateAll();
                            cache.put(0, 0);  // 保持非空
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(20, TimeUnit.SECONDS));
            // 最终状态:cache 应一致
            assertTrue(cache.size() <= 2000);
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C3-1: cleanup 在并发 put 下不抛异常")
    void probe_C3_cleanup_concurrent_put() throws Exception {
        CHCacheFactory factory = new CHCacheFactory();
        CHMCache<String, String> cache = factory.create();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch done = new CountDownLatch(4);
            for (int t = 0; t < 2; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 2000; i++) {
                            cache.put("k" + ((seed * 7 + i) % 100), "v", Duration.ofMillis(50));
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 200; i++) {
                            cache.cleanup();
                            Thread.sleep(1);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(20, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C4-1: metrics snapshot 在并发下不会抛异常或返回不一致数据")
    void probe_C4_metrics_concurrent() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(1000)
                .recordStats()
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            for (int i = 0; i < 100; i++) cache.put("k" + i, "v");
            CountDownLatch done = new CountDownLatch(4);
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 5000; i++) {
                            cache.put("k" + (i % 50), "v" + i);
                            cache.get("k" + (i % 50));
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            for (int t = 0; t < 2; t++) {
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < 1000; i++) {
                            CacheMetrics m = cache.metrics();
                            // 每次读都应不抛
                            assertNotNull(m);
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(20, TimeUnit.SECONDS));
            // 至少 hit + miss 总数合理
            CacheMetrics m = cache.metrics();
            assertTrue(m.hitCount() + m.missCount() > 0);
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C5-1: 高并发下 size 严格不超 maxSize")
    void probe_C5_size_invariant() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(200)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            int threads = 8;
            int ops = 10_000;
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                futures.add(pool.submit(() -> {
                    try {
                        barrier.await();
                        java.util.concurrent.ThreadLocalRandom r = java.util.concurrent.ThreadLocalRandom.current();
                        for (int i = 0; i < ops; i++) {
                            int key = r.nextInt(500);
                            if (r.nextBoolean()) cache.put(key, i);
                            else cache.get(key);
                            if (r.nextInt(100) == 0) cache.invalidate(key);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Future<?> f : futures) f.get(60, TimeUnit.SECONDS);
            assertTrue(cache.size() <= 200, "size=" + cache.size());
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C6-1: 并发 refresh 同一 key 不破坏 cache 状态")
    void probe_C6_concurrent_refresh_same_key() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            cache.put("k", "v0");
            int threads = 8;
            CyclicBarrier barrier = new CyclicBarrier(threads);
            AtomicInteger errors = new AtomicInteger();
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                futures.add(pool.submit(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < 100; i++) {
                            final int iter = i;
                            cache.refresh("k", key -> "v-" + seed + "-" + iter);
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    }
                }));
            }
            for (Future<?> f : futures) f.get(20, TimeUnit.SECONDS);
            Thread.sleep(500);  // 等所有 refresh 完成
            assertEquals(0, errors.get());
            String v = cache.get("k");
            // 应该是某个 v-<seed>-<i>
            assertTrue(v.startsWith("v-"), "should be refreshed, got: " + v);
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C7-1: 极端并发下 shutdown 仍能正确终止")
    void probe_C7_shutdown_under_load() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(10))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                pool.submit(() -> {
                    for (int i = 0; i < 2000; i++) {
                        cache.put("k" + ((seed * 31 + i) % 50), "v" + i);
                        cache.get("k" + i % 50);
                    }
                });
            }
            Thread.sleep(50);  // 让负载先跑一会
            cache.shutdown();
            // shutdown 后,其他线程的 put/get 应仍能完成(不抛)
            Thread.sleep(200);
            pool.shutdownNow();
            boolean terminated = pool.awaitTermination(2, TimeUnit.SECONDS);
            assertTrue(terminated);
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C8-1: invalidateIf 谓词在大量 key 上效率(无 N^2 退化)")
    void probe_C8_invalidateIf_perf() {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(10_000)
                .build();
        try {
            for (int i = 0; i < 5000; i++) cache.put(i, i);
            long start = System.nanoTime();
            cache.invalidateIf(k -> ((Integer) k) % 2 == 0);
            long elapsed = System.nanoTime() - start;
            assertTrue(elapsed < 2_000_000_000L, "should complete in < 2s, was " + elapsed + "ns");
            assertEquals(2500, cache.size(), "只剩奇数 key");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C9-1: 谓词中调用 cache.size() 等读方法不会死锁")
    void probe_C9_predicate_calls_cache() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .build();
        try {
            for (int i = 0; i < 50; i++) cache.put("k" + i, "v");
            // 谓词中调用 cache 读方法
            assertDoesNotThrow(() -> cache.invalidateIf(k -> {
                if (cache.size() > 100) return false;
                return k.equals("k10");
            }));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("C9-2: invalidateIf 谓词中调用 cache.invalidate 不死锁")
    void probe_C9_predicate_invalidate() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .build();
        try {
            for (int i = 0; i < 50; i++) cache.put("k" + i, "v");
            // 谓词中调用 invalidate —— 不会死锁(ConcurrentHashMap 弱一致)
            assertDoesNotThrow(() -> cache.invalidateIf(k -> {
                cache.invalidate("k0");
                return k.equals("k1");
            }));
        } finally {
            cache.shutdown();
        }
    }

    // Helper to avoid the awkward builder call from inside a lambda
    private static class CHCacheFactory {
        CHMCache<String, String> create() {
            return CHMCache.<String, String>newBuilder()
                    .maximumSize(1000)
                    .expireAfterWrite(Duration.ofMillis(50))
                    .cleanupInterval(Duration.ofMillis(20))
                    .build();
        }
    }
}
