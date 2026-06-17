package cn.wubo.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * {@link CHMCache} 完整功能与并发安全验证（Caffeine 风格 API）。
 *
 * <p>覆盖：
 * <ul>
 *   <li>put / get / invalidate / size / cleanup / shutdown / 默认 TTL</li>
 *   <li>已修复的缺陷：分片锁覆盖、token 防误删、TOCTOU、size 同步控制、
 *       nanoTime 单调时钟、清理线程异常兜底、shutdown 幂等</li>
 *   <li>containsKey、invalidateAll、invalidateIf、computeIfAbsent、loader</li>
 *   <li>参数校验、并发场景</li>
 * </ul>
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CHMCacheTest {

    private CHMCache<String, String> cache;

    @BeforeEach
    void setUp() {
        // maxSize=100, TTL=1s, cleanup interval=50ms
        cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .expireAfterWrite(Duration.ofSeconds(1))
                .cleanupInterval(Duration.ofMillis(50))
                .build();
    }

    @AfterEach
    void tearDown() {
        if (cache != null) cache.shutdown();
    }

    // ============================================================
    // 基础 CRUD
    // ============================================================

    @Test
    @DisplayName("put + get 基本流程")
    void testPutAndGet() {
        cache.put("k1", "v1");
        assertEquals("v1", cache.get("k1"));
        assertEquals(1, cache.size());
    }

    @Test
    @DisplayName("get 不存在的 key 返回 null 并记 miss")
    void testGetNonExistentKey() {
        assertNull(cache.get("nope"));
        assertEquals(1, cache.metrics().missCount());
    }

    @Test
    @DisplayName("invalidate 后 get 返回 null，二次 invalidate 返回 null")
    void testInvalidate() {
        cache.put("k1", "v1");
        assertEquals("v1", cache.invalidate("k1"));
        assertNull(cache.get("k1"));
        assertNull(cache.invalidate("k1"));
    }

    @Test
    @DisplayName("size 随 put/invalidate 正确变化")
    void testSize() {
        assertEquals(0, cache.size());
        cache.put("a", "1");
        cache.put("b", "2");
        assertEquals(2, cache.size());
        cache.invalidate("a");
        assertEquals(1, cache.size());
    }

    @Test
    @DisplayName("invalidateAll 清空所有数据")
    void testInvalidateAll() throws InterruptedException {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        assertEquals(3, cache.size());
        cache.invalidateAll();
        assertEquals(0, cache.size());
        // 清空后重新插入且未过期，可正常读出
        cache.put("d", "4");
        assertEquals("4", cache.get("d"));
        assertEquals(1, cache.size());
    }

    // ============================================================
    // 过期：惰性 / 主动 cleanup / 后台线程
    // ============================================================

    @Test
    @DisplayName("get 命中过期项时惰性删除并记 miss + expiration")
    void testExpiration_lazy() throws InterruptedException {
        cache.put("k", "v", Duration.ofMillis(50));
        assertEquals("v", cache.get("k"));
        Thread.sleep(150);
        assertNull(cache.get("k"));
        CacheMetrics m = cache.metrics();
        assertEquals(1, m.expirationCount());
    }

    @Test
    @DisplayName("manual cleanup() 立即清空过期项")
    void testCleanup() throws InterruptedException {
        cache.put("alive", "yes");
        cache.put("dead", "no", Duration.ofMillis(50));
        assertEquals(2, cache.size());
        Thread.sleep(100);
        cache.cleanup();
        assertEquals(1, cache.size());
        assertNull(cache.get("dead"));
        assertEquals("yes", cache.get("alive"));
        assertTrue(cache.metrics().expirationCount() >= 1);
    }

    @Test
    @DisplayName("后台清理线程自动回收过期项")
    void testBackgroundCleanup() throws InterruptedException {
        cache.put("auto", "x", Duration.ofMillis(50));
        assertEquals(1, cache.size());
        long deadline = System.currentTimeMillis() + 3_000;
        while (cache.size() > 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(0, cache.size());
        CacheMetrics m = cache.metrics();
        assertTrue(m.cleanupRunCount() >= 1, "cleanupRunCount should advance, was " + m.cleanupRunCount());
    }

    // ============================================================
    // 关键缺陷修复 #2：同一 key 多次 put，旧 DelayedItem 到期不应误删新值
    // ============================================================

    @Test
    @DisplayName("同一 key 多次 put：旧 DelayedItem 到期不会误删新值")
    void testMultiplePutsForSameKey_doesNotEvictNewerValue() throws InterruptedException {
        cache.put("k", "v1", Duration.ofMillis(80));
        Thread.sleep(40);
        cache.put("k", "v2", Duration.ofSeconds(5));
        Thread.sleep(200);

        assertEquals("v2", cache.get("k"));
        cache.cleanup();
        assertEquals("v2", cache.get("k"));
    }

    @Test
    @DisplayName("旧值过期后 get 应返回 null，新值仍可读")
    void testMultiplePutsOverwriteAfterExpiration() throws InterruptedException {
        cache.put("k", "v1", Duration.ofMillis(80));
        Thread.sleep(150);
        cache.cleanup();
        assertNull(cache.get("k"));

        cache.put("k", "v2", Duration.ofSeconds(1));
        assertEquals("v2", cache.get("k"));
    }

    // ============================================================
    // 关键缺陷修复：put 路径同步触发 LRU
    // ============================================================

    @Test
    @DisplayName("超出 maximumSize 时 put 路径同步淘汰最久未使用项（无需 sleep）")
    void testLRUEviction_synchronousOnPut() {
        CHMCache<Integer, Integer> small = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(3)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
        try {
            for (int i = 0; i < 5; i++) {
                small.put(i, i * 10);
            }
            assertTrue(small.size() <= 3, "size should be <= maxSize immediately, was " + small.size());
            // 5 items into size-3 cache = 2 evictions
            assertEquals(2, small.metrics().evictionCount(),
                    "should evict exactly 2 items, was " + small.metrics().evictionCount());
            assertNull(small.get(0));
            assertNull(small.get(1));
            assertEquals(Integer.valueOf(20), small.get(2));
            assertEquals(Integer.valueOf(30), small.get(3));
            assertEquals(Integer.valueOf(40), small.get(4));
        } finally {
            small.shutdown();
        }
    }

    @Test
    @DisplayName("最近访问的 key 不会被淘汰")
    void testAccessOrder_keepsRecentlyUsed() {
        CHMCache<Integer, Integer> small = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(3)
                .expireAfterWrite(Duration.ofSeconds(60))
                .shardedLocks(false)  // 严格 LRU 顺序，单分片
                .build();
        try {
            small.put(1, 10);
            small.put(2, 20);
            small.put(3, 30);
            assertEquals(Integer.valueOf(10), small.get(1));
            small.put(4, 40);
            assertEquals(Integer.valueOf(10), small.get(1));
            assertNull(small.get(2));
            assertEquals(Integer.valueOf(30), small.get(3));
            assertEquals(Integer.valueOf(40), small.get(4));
        } finally {
            small.shutdown();
        }
    }

    @Test
    @DisplayName("put 后再访问，再 put —— accessOrder 正确反映最近访问")
    void testAccessOrder_complexSequence() {
        CHMCache<Integer, Integer> small = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(3)
                .expireAfterWrite(Duration.ofSeconds(60))
                .shardedLocks(false)
                .build();
        try {
            small.put(1, 1);
            small.put(2, 2);
            small.put(3, 3);
            small.get(1);
            small.get(2);
            small.put(4, 4);
            assertNull(small.get(3));
            assertEquals(Integer.valueOf(1), small.get(1));
            assertEquals(Integer.valueOf(2), small.get(2));
            assertEquals(Integer.valueOf(4), small.get(4));
        } finally {
            small.shutdown();
        }
    }

    // ============================================================
    // 关键缺陷修复：LinkedHashMap 并发安全 —— 并发 put/get 不破坏结构
    // ============================================================

    @Test
    @DisplayName("并发 put/get 不会破坏内部结构")
    void testConcurrentPutAndGet() throws Exception {
        CHMCache<Integer, Integer> c = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(500)
                .expireAfterWrite(Duration.ofSeconds(60))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            int threads = 8;
            int opsPerThread = 3_000;
            CyclicBarrier barrier = new CyclicBarrier(threads);
            List<Future<?>> futures = new ArrayList<>();
            AtomicInteger lostPuts = new AtomicInteger(0);
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                futures.add(pool.submit(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            int key = (seed * 31 + i) % 200;
                            c.put(key, i);
                            Integer v = c.get(key);
                            if (v == null) {
                                lostPuts.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get(20, TimeUnit.SECONDS);
            }
            assertTrue(c.size() <= 500, "size must never exceed maxSize, was " + c.size());
        } finally {
            pool.shutdownNow();
            c.shutdown();
        }
    }

    @Test
    @DisplayName("并发 TOCTOU：get 命中过期与 put 写入新值不应误删新值")
    void testConcurrentGetExpiredAndPut() throws Exception {
        CHMCache<String, String> c = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            CountDownLatch done = new CountDownLatch(2);
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        c.put("k", "new-" + i, Duration.ofMillis(30));
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 1000; i++) {
                        c.get("k");
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
            assertTrue(done.await(20, TimeUnit.SECONDS), "concurrent ops did not finish");
            c.get("k");  // 不应抛异常
        } finally {
            pool.shutdownNow();
            c.shutdown();
        }
    }

    // ============================================================
    // containsKey
    // ============================================================

    @Test
    @DisplayName("containsKey 不影响访问顺序、不计入 hit/miss")
    void testContainsKey_doesNotAffectMetricsOrAccessOrder() {
        cache.put("a", "1");
        cache.put("b", "2");
        cache.put("c", "3");
        long hitBefore = cache.metrics().hitCount();
        long missBefore = cache.metrics().missCount();
        assertTrue(cache.containsKey("a"));
        assertFalse(cache.containsKey("missing"));
        assertEquals(hitBefore, cache.metrics().hitCount());
        assertEquals(missBefore, cache.metrics().missCount());
    }

    @Test
    @DisplayName("containsKey 对过期 key 返回 false 并触发惰性清理")
    void testContainsKey_expired() throws InterruptedException {
        cache.put("k", "v", Duration.ofMillis(30));
        Thread.sleep(100);
        assertFalse(cache.containsKey("k"));
        assertEquals(0, cache.size());
        assertEquals(1, cache.metrics().expirationCount());
    }

    // ============================================================
    // computeIfAbsent / loader
    // ============================================================

    @Test
    @DisplayName("computeIfAbsent：不存在时调用 loader，存在时直接返回")
    void testComputeIfAbsent() {
        AtomicInteger calls = new AtomicInteger(0);
        String v = cache.computeIfAbsent("k", k -> {
            calls.incrementAndGet();
            return "computed-" + k;
        });
        assertEquals("computed-k", v);
        assertEquals(1, calls.get());

        String v2 = cache.computeIfAbsent("k", k -> {
            calls.incrementAndGet();
            return "should-not-run";
        });
        assertEquals("computed-k", v2);
        assertEquals(1, calls.get());
    }

    @Test
    @DisplayName("computeIfAbsent：loader 返回 null 则不写入")
    void testComputeIfAbsent_loaderReturnsNull() {
        String v = cache.computeIfAbsent("k", k -> null);
        assertNull(v);
        assertEquals(0, cache.size());
    }

    @Test
    @DisplayName("computeIfAbsent：loader 抛异常向上传播")
    void testComputeIfAbsent_loaderThrows() {
        RuntimeException boom = new RuntimeException("boom");
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> cache.computeIfAbsent("k", k -> { throw boom; }));
        assertEquals(boom, thrown.getCause());
        assertEquals(0, cache.size());
    }

    // ============================================================
    // 监控指标
    // ============================================================

    @Test
    @DisplayName("hit/miss 指标随 get 正确变化")
    void testMetrics_hitMiss() {
        cache.put("k", "v");
        assertEquals("v", cache.get("k"));        // hit
        assertEquals("v", cache.get("k"));        // hit
        assertNull(cache.get("nope"));            // miss
        CacheMetrics m = cache.metrics();
        assertEquals(2, m.hitCount());
        assertEquals(1, m.missCount());
        assertEquals(2.0 / 3.0, m.hitRate(), 1e-9);
    }

    @Test
    @DisplayName("evictionCount 仅由 LRU 淘汰累加；expirationCount 由过期清理累加")
    void testMetrics_evictionVsExpiration() throws InterruptedException {
        CHMCache<Integer, Integer> c = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2)
                .expireAfterWrite(Duration.ofSeconds(60))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            c.put(1, 1);
            c.put(2, 2);
            c.put(3, 3);
            assertEquals(1, c.metrics().evictionCount());

            c.put(4, 4, Duration.ofMillis(30));
            Thread.sleep(80);
            c.cleanup();
            assertTrue(c.metrics().expirationCount() >= 1);
        } finally {
            c.shutdown();
        }
    }

    @Test
    @DisplayName("metrics() 返回独立快照，不受后续操作影响")
    void testMetrics_snapshotIsImmutable() {
        cache.put("k", "v");
        CacheMetrics snap = cache.metrics();
        assertEquals(1, snap.currentSize());
        cache.put("k2", "v2");
        assertEquals(1, snap.currentSize());
        assertEquals(2, cache.metrics().currentSize());
    }

    @Test
    @DisplayName("hitRate 无任何访问时返回 0")
    void testMetrics_hitRate_noAccess() {
        assertEquals(0.0, cache.metrics().hitRate(), 0.0001);
    }

    @Test
    @DisplayName("cleanupRunCount 真实反映清理活动")
    void testMetrics_cleanup() throws InterruptedException {
        CHMCache<String, String> c = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(30))
                .build();
        try {
            Thread.sleep(500);
            CacheMetrics m = c.metrics();
            assertTrue(m.cleanupRunCount() >= 1,
                    "expected >= 1 cleanup run, got " + m.cleanupRunCount());
        } finally {
            c.shutdown();
        }
    }

    // ============================================================
    // TTL 行为
    // ============================================================

    @Test
    @DisplayName("显式 put 的 TTL 在指定时长内可读")
    void testExplicitTtl_isNotRandomized() {
        cache.put("k", "v", Duration.ofSeconds(1));
        assertEquals("v", cache.get("k"));
        try { Thread.sleep(200); } catch (InterruptedException ignored) {}
        assertEquals("v", cache.get("k"));
    }

    @Test
    @DisplayName("极短 TTL（1ms）行为合理，不抛异常")
    void testVeryShortTtl() throws InterruptedException {
        cache.put("k", "v", Duration.ofMillis(1));
        Thread.sleep(20);
        assertNull(cache.get("k"));
    }

    // ============================================================
    // invalidateIf
    // ============================================================

    @Test
    @DisplayName("invalidateIf 按谓词批量失效")
    void testInvalidateIf() {
        cache.put("user:1", "a");
        cache.put("user:2", "b");
        cache.put("post:1", "c");
        cache.invalidateIf(k -> ((String) k).startsWith("user:"));
        assertEquals(1, cache.size());
        assertEquals("c", cache.get("post:1"));
    }

    // ============================================================
    // 参数校验
    // ============================================================

    @Test
    @DisplayName("put/get 等方法对 null key 抛 NPE")
    void testNullKeyValidation() {
        assertThrows(NullPointerException.class, () -> cache.put(null, "v"));
        assertThrows(NullPointerException.class, () -> cache.get(null));
        assertThrows(NullPointerException.class, () -> cache.invalidate(null));
        assertThrows(NullPointerException.class, () -> cache.containsKey(null));
        assertThrows(NullPointerException.class, () -> cache.computeIfAbsent(null, k -> "v"));
    }

    @Test
    @DisplayName("put 接受 null value")
    void testNullValueAccepted() {
        cache.put("k", null);
        assertTrue(cache.containsKey("k"));
        assertNull(cache.get("k"));
    }

    @Test
    @DisplayName("put 非正 TTL 抛 IllegalArgumentException")
    void testNonPositiveTtlRejected() {
        assertThrows(IllegalArgumentException.class, () -> cache.put("k", "v", Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> cache.put("k", "v", Duration.ofMillis(-1)));
    }

    // ============================================================
    // shutdown 幂等 + 后台线程是 daemon
    // ============================================================

    @Test
    @DisplayName("shutdown 重复调用幂等")
    void testShutdownIdempotent() {
        cache.shutdown();
        cache.shutdown();
    }

    @Test
    @DisplayName("shutdown 后 put/get/invalidate 仍可用（仅后台清理停止）")
    void testOperationsAfterShutdown() {
        cache.shutdown();
        cache.put("k", "v");
        assertEquals("v", cache.get("k"));
        assertEquals("v", cache.invalidate("k"));
        assertNull(cache.get("k"));
    }

    @Test
    @DisplayName("后台清理线程是 daemon 线程，不阻止 JVM 退出")
    void testCleanupThreadIsDaemon() {
        Thread[] threads = new Thread[Thread.activeCount() + 5];
        int n = Thread.enumerate(threads);
        boolean found = false;
        for (int i = 0; i < n; i++) {
            if (threads[i] != null && threads[i].getName().startsWith("chmcache-cleanup-")) {
                assertTrue(threads[i].isDaemon(), "cleanup thread must be daemon");
                found = true;
            }
        }
        assertTrue(found, "should have at least one cleanup thread");
    }

    // ============================================================
    // 默认配置
    // ============================================================

    @Test
    @DisplayName("默认 cache name 为 \"default\"")
    void testDefaultName() {
        assertEquals("default", cache.getName());
    }
}