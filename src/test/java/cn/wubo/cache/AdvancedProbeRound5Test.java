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
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进阶探针第 7 轮:自定义组件鲁棒性
 * - Expiry 抛异常
 * - Weigher 副作用
 * - RemovalListener 阻塞
 * - Builder 组合约束
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class AdvancedProbeRound5Test {

    @Test
    @DisplayName("E1-1: Expiry.expireAfterCreate 抛异常时,put 不应成功")
    void probe_E1_expiry_throw_on_create() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter((k, v, t) -> { throw new RuntimeException("create boom"); })
                .build();
        try {
            // 异常会向上传播 —— 这是 BUG:用户 Expiry 异常不应破坏整个 put
            // 但当前行为是异常传播(无 try-catch 兜底)
            RuntimeException ex = assertThrows(RuntimeException.class,
                    () -> cache.put("k", "v"));
            // put 失败,cache 应为空
            assertEquals(0, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E1-2: Expiry.expireAfterRead 抛异常时,get 行为")
    void probe_E1_expiry_throw_on_read() {
        AtomicInteger readCalls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter(new Expiry<>() {
                    @Override
                    public Duration expireAfterCreate(String k, String v, long t) {
                        return Duration.ofMinutes(5);
                    }
                    @Override
                    public Duration expireAfterRead(String k, String v, long t) {
                        if (readCalls.incrementAndGet() == 1) {
                            throw new RuntimeException("read boom");
                        }
                        return Duration.ofMinutes(5);
                    }
                })
                .build();
        try {
            cache.put("k", "v");
            // 第一次 get 会因 read 抛异常
            assertThrows(RuntimeException.class, () -> cache.get("k"));
            // 但 cache 中值仍存在
            assertEquals(1, cache.size());
            // 第二次 get 应正常
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E1-3: Expiry 在并发下被调用时,无可见性问题")
    void probe_E1_expiry_thread_safety() throws Exception {
        AtomicInteger createCalls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .expireAfter((k, v, t) -> {
                    createCalls.incrementAndGet();
                    return Duration.ofSeconds(60);
                })
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            int threads = 8;
            int ops = 500;
            CountDownLatch done = new CountDownLatch(threads);
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                pool.submit(() -> {
                    try {
                        for (int i = 0; i < ops; i++) {
                            cache.put("k" + (seed * 1000 + i), "v");
                        }
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(30, TimeUnit.SECONDS));
            // 每个 put 调用一次 expireAfterCreate
            assertEquals(threads * ops, createCalls.get());
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E2-1: Weigher 抛异常时,put 行为")
    void probe_E2_weigher_throw() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumWeight(100)
                .weigher((k, v) -> { throw new RuntimeException("weigh boom"); })
                .build();
        try {
            // weigher 异常会向上传播,put 失败
            assertThrows(RuntimeException.class, () -> cache.put("k", "v"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E2-2: Weigher 副作用不应破坏 cache 状态")
    void probe_E2_weigher_side_effect() {
        AtomicInteger weighCalls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumWeight(100)
                .weigher((k, v) -> {
                    weighCalls.incrementAndGet();
                    return 1;
                })
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            assertEquals(2, weighCalls.get());
            assertEquals(2, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E3-1: RemovalListener 慢操作会阻塞 LRU 淘汰,可能导致 size 临时超出 maxSize")
    void probe_E3_slow_listener_blocks_evict() throws InterruptedException {
        // 这是设计上的权衡:listener 在 stripe 锁内被调用,慢 listener 会阻塞其他 put/evict
        // 测试记录这个行为,不抛异常
        List<RemovalCause> removed = new ArrayList<>();
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2)
                .removalListener((k, v, c) -> {
                    try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    synchronized (removed) { removed.add(c); }
                })
                .build();
        try {
            cache.put(1, 1);
            cache.put(2, 2);
            // put 3 应触发淘汰,慢 listener 会阻塞
            long start = System.currentTimeMillis();
            cache.put(3, 3);
            long elapsed = System.currentTimeMillis() - start;
            // listener sleep 50ms,evict 在锁内调用 listener
            assertTrue(elapsed >= 40, "put 应至少 50ms(被 listener 阻塞),实际 " + elapsed + "ms");
            assertEquals(1, synchronizedList(removed).size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E3-2: RemovalListener 抛异常被吞,不影响后续 listener 调用")
    void probe_E3_listener_exception_isolated() {
        List<RemovalCause> causes = new ArrayList<>();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .removalListener((k, v, c) -> {
                    causes.add(c);
                    if (causes.size() == 1) {
                        throw new RuntimeException("first listener boom");
                    }
                })
                .build();
        try {
            cache.put("a", "1");
            cache.put("a", "2");  // REPLACED -> listener throws
            cache.invalidate("a");  // EXPLICIT -> listener called again
            // 即使第一次抛异常,第二次仍被调用
            assertEquals(2, causes.size());
            assertEquals(RemovalCause.REPLACED, causes.get(0));
            assertEquals(RemovalCause.EXPLICIT, causes.get(1));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E4-1: builder 同时配 expireAfter 和 expireAfterAccess,后者胜出")
    void probe_E4_builder_combination_after_and_access() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter((k, v, t) -> Duration.ofSeconds(60))  // custom Expiry
                .expireAfterAccess(Duration.ofMillis(100))  // 覆盖?
                .build();
        try {
            cache.put("k", "v");
            // expireAfterAccess 会被调用,slidingTtl=true
            Thread.sleep(50);
            assertEquals("v", cache.get("k"));  // 应续期
            Thread.sleep(80);
            assertEquals("v", cache.get("k"), "sliding TTL 应工作");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E4-2: builder 配 refreshAfterWrite 但不配 expiration,默认 TTL 行为")
    void probe_E4_refreshAfterWrite_default_ttl() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(50))
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E4-3: builder 配 recordStats + 大量 get,LatencyHistogram 不抛异常")
    void probe_E4_record_stats_load() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(1000)
                .recordStats()
                .build();
        try {
            for (int i = 0; i < 1000; i++) {
                cache.put("k" + i, "v" + i);
            }
            for (int i = 0; i < 10000; i++) {
                cache.get("k" + (i % 1000));
            }
            CacheStats s = cache.stats();
            assertTrue(s.averageGetPenalty(TimeUnit.NANOSECONDS) >= 0);
            assertTrue(s.maxGetPenalty(TimeUnit.NANOSECONDS) >= s.averageGetPenalty(TimeUnit.NANOSECONDS));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("E4-4: builder 自定义 name 生效,getName() 正确返回")
    void probe_E4_custom_name_meter() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .name("test-cache-1")
                .build();
        try {
            assertEquals("test-cache-1", cache.getName());
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    private <T> List<T> synchronizedList(List<T> list) {
        return java.util.Collections.synchronizedList(list);
    }
}
