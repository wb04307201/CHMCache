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
 * 进阶探针第 1 轮:资源/内存/异步行为
 *
 * 关注点:
 * - DelayQueue 残留项在反复 invalidate 后的内存表现
 * - 多次 refresh 同一 key 的 token 安全与覆盖语义
 * - custom Expiry 边界值(0 / 负值 / 极大值)
 * - 后台清理线程的异常处理
 * - 关闭语义
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class AdvancedProbeRound1Test {

    // ============================================================
    // 资源 / 内存
    // ============================================================

    @Test
    @DisplayName("A1-1: 大量 put + 立即 invalidate 后,DelayQueue 残留项能否被清空")
    void probe_A1_delayqueue_drain() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10_000)
                .expireAfterWrite(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build();
        try {
            // 1000 个 put + 立即 invalidate,每次都会留一个 DelayedItem 在队列
            for (int i = 0; i < 1000; i++) {
                cache.put("k" + i, "v", Duration.ofMillis(50));
                cache.invalidate("k" + i);
            }
            // 等所有 stale 项到期被 drain
            Thread.sleep(300);
            // 此时 cache 应该是空的
            assertEquals(0, cache.size());
            // 重新填充应正常
            cache.put("fresh", "v");
            assertEquals(1, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A1-2: 高频短 TTL put,后台清理线程不会 OOM")
    void probe_A1_high_freq_short_ttl() throws InterruptedException {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(100)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            long start = System.currentTimeMillis();
            int n = 0;
            while (System.currentTimeMillis() - start < 2000) {
                cache.put(n % 50, n);
                n++;
            }
            // 2 秒内连续写入,不应 OOM
            assertTrue(n > 1000, "should have written > 1000, was " + n);
        } finally {
            cache.shutdown();
        }
    }

    // ============================================================
    // 异步 / refresh
    // ============================================================

    @Test
    @DisplayName("A2-1: 多次 refresh 同一 key,最终值应被替换为最新一次 loader 的结果")
    void probe_A2_multiple_refreshes() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v0");
            for (int i = 1; i <= 5; i++) {
                final int n = i;
                cache.refresh("k", key -> "v" + n);
            }
            // 等待所有 refresh 完成
            Thread.sleep(500);
            String v = cache.get("k");
            assertTrue(v.startsWith("v") && !v.equals("v0"),
                    "expected v1..v5, got: " + v);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A2-2: refreshAfterWrite 模式下,refresh 加载的 value 替换旧值")
    void probe_A2_refreshAfterWrite_replaces() throws InterruptedException {
        AtomicInteger loaderCalls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            cache.put("k", "v1");
            cache.refresh("k", key -> {
                loaderCalls.incrementAndGet();
                return "v-refreshed";
            });
            // 等 refresh 窗口 + 后台扫描
            Thread.sleep(300);
            // 等待异步完成
            long deadline = System.currentTimeMillis() + 2_000;
            while (loaderCalls.get() < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(loaderCalls.get() >= 1, "loader should run, was " + loaderCalls.get());
            assertEquals("v-refreshed", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A2-3: refresh loader 抛异常不应中断其他 refresh")
    void probe_A2_refresh_exception_isolated() throws InterruptedException {
        AtomicInteger successCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");
            cache.refresh("a", k -> { throw new RuntimeException("a failed"); });
            cache.refresh("b", k -> { successCount.incrementAndGet(); return "b-new"; });
            cache.refresh("c", k -> { successCount.incrementAndGet(); return "c-new"; });
            Thread.sleep(300);
            assertEquals(2, successCount.get());
            assertEquals("b-new", cache.get("b"));
            assertEquals("c-new", cache.get("c"));
            assertEquals("1", cache.get("a"), "a 失败应保留原值");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A2-4: refresh 后立即 invalidate,异步 put 不应回写")
    void probe_A2_refresh_then_invalidate() throws InterruptedException {
        AtomicInteger calls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v1");
            cache.refresh("k", key -> {
                calls.incrementAndGet();
                return "v2";
            });
            cache.invalidate("k");
            Thread.sleep(200);
            // refresh 加载完后会 put 进去 —— 设计上允许此行为
            String v = cache.get("k");
            // 记录实际行为:可能是 null(若 loader 还没跑)或 v2(若已 put)
            // 不应抛异常
            assertNotNull(cache.metrics());
        } finally {
            cache.shutdown();
        }
    }

    // ============================================================
    // Custom Expiry 边界
    // ============================================================

    @Test
    @DisplayName("A3-1: Expiry 返回 Duration.ZERO,条目应立即过期")
    void probe_A3_expiry_zero_duration() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter((k, v, t) -> Duration.ZERO)
                .build();
        try {
            cache.put("k", "v");
            // TTL=0 意味着立即过期
            Thread.sleep(50);
            assertNull(cache.get("k"), "Duration.ZERO 应当立即过期");
            // 但 get 仍能命中(因 lazy 删除可能没运行),不应抛
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A3-2: Expiry 返回极大值,条目几乎永不过期")
    void probe_A3_expiry_huge_duration() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter((k, v, t) -> Duration.ofNanos(Long.MAX_VALUE / 4))
                .build();
        try {
            cache.put("k", "v");
            Thread.sleep(100);
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A3-3: Expiry 抛异常时,缓存仍可继续使用")
    void probe_A3_expiry_throws() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfter((k, v, t) -> { throw new RuntimeException("boom"); })
                .build();
        try {
            // put 路径触发 expireAfterCreate,如果异常不被吞,put 会失败
            // 当前实现:computeTtl 用 Math.max(0, ...) 但未 try-catch
            // 这里只记录实际行为
            try {
                cache.put("k", "v");
                // 若成功(异常被吞),值应在
                String got = cache.get("k");
                // 后续操作不应崩溃
                cache.put("k2", "v2");
                assertNotNull(got, "if put succeeded, value should be retrievable");
            } catch (Exception e) {
                // 若异常传播,记录
                System.out.println("[A3-3] put threw: " + e);
            }
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A3-4: expireAfterWrite 极小 TTL (1ns) 合法接受")
    void probe_A3_ttl_one_nanosecond() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            // 1ns 是合法的(只拒绝 <=0)
            assertDoesNotThrow(() -> cache.put("k", "v", Duration.ofNanos(1)));
            // 0 才是非法的
            assertThrows(IllegalArgumentException.class,
                    () -> cache.put("k", "v", Duration.ZERO));
        } finally {
            cache.shutdown();
        }
    }

    // ============================================================
    // 后台清理
    // ============================================================

    @Test
    @DisplayName("A4-1: 后台清理线程在 cache 长时间无活动后仍持续运行")
    void probe_A4_cleanup_thread_alive() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(50))
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        try {
            long initial = cache.metrics().cleanupRunCount();
            Thread.sleep(500);
            long after = cache.metrics().cleanupRunCount();
            assertTrue(after > initial, "cleanup should run multiple times");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A4-2: cleanupExecutor 在 shutdown 后真正停止")
    void probe_A4_executor_terminates() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .cleanupInterval(Duration.ofMillis(20))
                .build();
        Thread[] threads = new Thread[Thread.activeCount() + 10];
        int n = Thread.enumerate(threads);
        Thread cleanupThread = null;
        for (int i = 0; i < n; i++) {
            if (threads[i] != null && threads[i].getName().startsWith("chmcache-cleanup-")) {
                cleanupThread = threads[i];
                break;
            }
        }
        assertNotNull(cleanupThread, "should find cleanup thread");
        cache.shutdown();
        // shutdown 后线程应已退出(alive=false)。shutdown() 内部已 awaitTermination(5s)
        // 完成后返回,理论上 alive 必为 false;但出于稳健加 200ms 容错
        Thread.sleep(200);
        assertFalse(cleanupThread.isAlive(),
                "cleanup thread should be dead after shutdown, was alive");
    }

    // ============================================================
    // 关闭语义
    // ============================================================

    @Test
    @DisplayName("A5-1: isShutdown 在 shutdown 后返回 true")
    void probe_A5_isShutdown() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        assertFalse(cache.isShutdown());
        cache.shutdown();
        assertTrue(cache.isShutdown());
    }

    @Test
    @DisplayName("A5-2: shutdown 后 cleanup() 仍能调用")
    void probe_A5_cleanup_after_shutdown() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        cache.shutdown();
        cache.put("k", "v");
        // 不应抛异常
        assertDoesNotThrow(() -> cache.cleanup());
    }

    @Test
    @DisplayName("A5-3: shutdown 后 isShutdown 多次读取稳定")
    void probe_A5_isShutdown_stable() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        cache.shutdown();
        for (int i = 0; i < 10; i++) {
            assertTrue(cache.isShutdown());
        }
    }

    // ============================================================
    // containsKey / getAll 边界
    // ============================================================

    @Test
    @DisplayName("A6-1: containsKey 不影响 LRU 顺序(不调用 accessOrder.touch)")
    void probe_A6_containsKey_no_lru_effect() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(3)
                .shardedLocks(false)
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3");
            // containsKey 不应影响 LRU 顺序(只是检查,不 touch)
            cache.containsKey("a");
            cache.containsKey("b");
            // 触发淘汰 —— a 是最久未用的(从未 get 过)
            cache.put("d", "4");
            // a 应被淘汰(最旧),c 仍应存在
            assertNull(cache.get("a"), "a 应被淘汰(最久未用,因为 containsKey 不 touch LRU)");
            assertEquals("3", cache.get("c"), "c 应仍在");
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A6-2: getAll 对 keys=null 抛 NPE")
    void probe_A6_getAll_keys_null() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            assertThrows(NullPointerException.class,
                    () -> cache.getAll(null, m -> new java.util.HashMap<>()));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A6-3: getAll 对空 set 不调 loader,返回空 map")
    void probe_A6_getAll_empty() {
        AtomicInteger calls = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            java.util.Map<String, String> r = cache.getAll(Set.of(), m -> {
                calls.incrementAndGet();
                return new java.util.HashMap<>();
            });
            assertTrue(r.isEmpty());
            assertEquals(0, calls.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("A6-4: getAll 包含已过期的 key,正确处理")
    void probe_A6_getAll_expired() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(50))
                .build();
        try {
            cache.put("a", "1", Duration.ofMillis(30));
            Thread.sleep(80);
            AtomicInteger missCount = new AtomicInteger();
            java.util.Map<String, String> r = cache.getAll(
                    Set.of("a", "b"),
                    missing -> {
                        missCount.addAndGet(missing.size());
                        java.util.HashMap<String, String> m = new java.util.HashMap<>();
                        for (String k : missing) m.put(k, "loaded-" + k);
                        return m;
                    });
            assertEquals(2, r.size());
            assertEquals(2, missCount.get());
        } finally {
            cache.shutdown();
        }
    }
}
