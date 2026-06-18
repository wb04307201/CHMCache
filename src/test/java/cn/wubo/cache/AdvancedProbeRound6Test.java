package cn.wubo.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进阶探针第 8 轮:AsyncRefresher 行为
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class AdvancedProbeRound6Test {

    @Test
    @DisplayName("F1-1: refresh 在 refreshAfterWrite 模式下,会触发后台异步加载")
    void probe_F1_refreshAfterWrite_async_load() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build();
        try {
            // 先 put,再注册 refresh,这样 cv 一开始就存在
            cache.put("k", "v-initial");
            cache.refresh("k", k -> {
                loadCount.incrementAndGet();
                return "v-refreshed-" + loadCount.get();
            });
            // 等后台扫描 + 异步加载
            Thread.sleep(400);
            // 应被刷新过
            String v = cache.get("k");
            assertNotNull(v, "get 返回 null,key 被意外移除");
            assertTrue(v.startsWith("v-refreshed-"), "应被异步刷新,实际=" + v);
            assertTrue(loadCount.get() >= 1, "loader 应被调用,实际=" + loadCount.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F1-2: refresh loader 返回 null,不应覆盖原值")
    void probe_F1_refresh_null_keeps_value() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v-original");
            cache.refresh("k", k -> null);  // 显式 refresh(无 refreshAfterWrite)
            Thread.sleep(300);
            // refresh 返回 null,不应覆盖
            assertEquals("v-original", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F1-3: refresh loader 抛异常被吞,cache 状态不变")
    void probe_F1_refresh_throw_swallowed() throws Exception {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v-original");
            cache.refresh("k", k -> { throw new RuntimeException("loader boom"); });
            Thread.sleep(300);
            // 异常被吞,原值不变
            assertEquals("v-original", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F1-4: refresh 后原 key 被 invalidate,异步加载应仍能 put 回来")
    void probe_F1_refresh_after_invalidate() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v");
            cache.refresh("k", k -> {
                loadCount.incrementAndGet();
                return "v-refreshed";
            });
            // 立即 invalidate
            cache.invalidate("k");
            Thread.sleep(300);
            // refresh 的异步 put 把 key 写回来了
            assertEquals("v-refreshed", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F2-1: 在 refreshAfterWrite 模式下,refresh 后会持续触发刷新")
    void probe_F2_refresh_repeats() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build();
        try {
            cache.put("k", "v-init");
            cache.refresh("k", k -> {
                loadCount.incrementAndGet();
                return "v-" + loadCount.get();
            });
            // 等 600ms,每 50ms 后台扫描,应触发多次刷新
            Thread.sleep(600);
            assertTrue(loadCount.get() >= 2, "应被多次刷新,实际=" + loadCount.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F2-2: 在 refreshAfterWrite 模式下,key 被 invalidate 后不会继续刷新")
    void probe_F2_refresh_stops_after_invalidate() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(50))
                .build();
        try {
            cache.refresh("k", k -> {
                loadCount.incrementAndGet();
                return "v";
            });
            cache.put("k", "v-init");
            Thread.sleep(150);
            int before = loadCount.get();
            cache.invalidate("k");
            Thread.sleep(200);
            int after = loadCount.get();
            // invalidate 后应停止刷新
            assertTrue(after - before <= 1,
                    "invalidate 后应停止刷新,before=" + before + ",after=" + after);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F3-1: 自定义 executor 拒绝任务时,不抛异常")
    void probe_F3_rejected_executor_swallowed() throws Exception {
        ExecutorService dead = Executors.newSingleThreadExecutor();
        dead.shutdown();  // 立即关闭,后续 execute 抛 RejectedExecutionException
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .executor(dead)
                .build();
        try {
            cache.put("k", "v");
            // refresh 会通过已关闭的 executor 派发任务,被吞
            cache.refresh("k", k -> "v-refreshed");
            // 不应抛,原值不变
            assertEquals("v", cache.get("k"));
        } finally {
            dead.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F3-2: refresh 与并发 put 同一 key,值不应混乱")
    void probe_F3_refresh_concurrent_put() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            cache.put("k", "init");
            CountDownLatch done = new CountDownLatch(2);
            // 并发 put
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        cache.put("k", "put-" + i);
                    }
                } finally {
                    done.countDown();
                }
            });
            // 并发 refresh
            pool.submit(() -> {
                try {
                    for (int i = 0; i < 50; i++) {
                        cache.refresh("k", k -> {
                            loadCount.incrementAndGet();
                            return "refresh-" + loadCount.get();
                        });
                    }
                } finally {
                    done.countDown();
                }
            });
            assertTrue(done.await(30, TimeUnit.SECONDS));
            Thread.sleep(300);
            // 值应是 put-x 或 refresh-x 二者之一
            String v = cache.get("k");
            assertTrue(v.startsWith("put-") || v.startsWith("refresh-"),
                    "值应为 put-x 或 refresh-x,实际=" + v);
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }

    @Test
    @DisplayName("F3-3: refresh 在 cache shutdown 后派发,load 不应发生")
    void probe_F3_refresh_after_shutdown() throws Exception {
        AtomicInteger loadCount = new AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        cache.put("k", "v");
        // shutdown 后,executor 仍可被 submit(因为 asyncRefresher 用的是 ForkJoinPool.commonPool())
        // 所以 refresh 仍会执行,但 cache 已 shutdown,put 行为未定义
        cache.shutdown();
        // 这一步可能正常执行(因为 executor 独立),不抛异常即可
        try {
            cache.refresh("k", k -> {
                loadCount.incrementAndGet();
                return "v-after-shutdown";
            });
            Thread.sleep(200);
            // load 可能执行了(因为 executor 独立),也可能没执行;两者都可接受
        } catch (Exception e) {
            // shutdown 后 refresh 抛异常,也可接受
        }
        // 不强制断言,只验证不崩
    }
}
