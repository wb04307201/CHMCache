package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ExpirationPolicyTest {

    @Test
    void testAfterWrite_ExpiresAfterTtl() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofMillis(80))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
            Thread.sleep(150);
            assertNull(cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testAfterAccess_SlidesTtlOnGet() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterAccess(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            cache.put("k", "v");
            // 多次访问延长 TTL
            for (int i = 0; i < 5; i++) {
                Thread.sleep(60);
                assertEquals("v", cache.get("k"));
            }
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefreshAfterWrite_DoesNotImmediatelyExpire() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofSeconds(10))
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testCustomExpiry_TakesPrecedence() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofSeconds(60))
                .expireAfter((k, v, t) -> Duration.ofMillis(50))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
            Thread.sleep(120);
            assertNull(cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testCustomExpiry_ReadHook() throws InterruptedException {
        java.util.concurrent.atomic.AtomicInteger readCalls = new java.util.concurrent.atomic.AtomicInteger();
        Expiry<String, String> expiry = new Expiry<>() {
            @Override
            public Duration expireAfterCreate(String k, String v, long t) {
                return Duration.ofSeconds(60);
            }
            @Override
            public Duration expireAfterRead(String k, String v, long t) {
                readCalls.incrementAndGet();
                return Duration.ofSeconds(60);
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
            // D10 修复后：自定义 expireAfter 也启用 sliding TTL，
            // 每次 get 命中 + put 时 expireAfterRead 都会被调用
            assertEquals(3, readCalls.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testExpireAfterAccess_PutAlsoRefreshesTtl() throws InterruptedException {
        // D10 回归测试：expireAfterAccess 模式下，put 也应刷新 TTL
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterAccess(Duration.ofMillis(150))
                .cleanupInterval(Duration.ofSeconds(60))
                .build();
        try {
            cache.put("k", "v1");
            Thread.sleep(100);
            // 此时 put 重置 TTL
            cache.put("k", "v2");
            Thread.sleep(100);
            // 如果 put 不刷新 TTL，原值已过期，应为 null；修复后应为 "v2"
            assertEquals("v2", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefreshAfterWrite_BackgroundRefreshes() throws InterruptedException {
        // D11 回归测试：refreshAfterWrite 模式下，写后 N 秒后台异步刷新
        java.util.concurrent.atomic.AtomicInteger loaderCalls = new java.util.concurrent.atomic.AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .refreshAfterWrite(Duration.ofMillis(100))
                .cleanupInterval(Duration.ofMillis(50))
                .build();
        try {
            cache.put("k", "v1");
            cache.refresh("k", key -> {
                loaderCalls.incrementAndGet();
                return "v-refreshed";
            });
            // 等过 refresh 窗口并让后台跑几个周期
            Thread.sleep(400);
            // 等待后台异步刷新完成（至少触发 1 次）
            long deadline = System.currentTimeMillis() + 2_000;
            while (loaderCalls.get() < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertTrue(loaderCalls.get() >= 1,
                    "loader should be called at least once, was " + loaderCalls.get());
            assertEquals("v-refreshed", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }
}