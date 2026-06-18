package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

@org.junit.jupiter.api.Timeout(value = 10, unit = java.util.concurrent.TimeUnit.SECONDS)
class CHMCacheBuilderTest {

    @Test
    void testDefaults() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            assertEquals("default", cache.getName());
            assertNotNull(cache);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testName() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .name("users")
                .maximumSize(10)
                .build();
        try {
            assertEquals("users", cache.getName());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testMaximumSizeMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMCache.newBuilder().maximumSize(0));
        assertThrows(IllegalArgumentException.class,
                () -> CHMCache.newBuilder().maximumSize(-1));
    }

    @Test
    void testMaximumWeightMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMCache.newBuilder().maximumWeight(0));
    }

    @Test
    void testMustConfigureSizeOrWeight() {
        assertThrows(IllegalStateException.class,
                () -> CHMCache.newBuilder().build());
    }

    @Test
    void testCannotConfigureBothSizeAndWeight() {
        assertThrows(IllegalStateException.class,
                () -> CHMCache.<String, String>newBuilder()
                        .maximumSize(10)
                        .maximumWeight(100));
    }

    @Test
    void testCleanupIntervalMustBePositive() {
        assertThrows(IllegalArgumentException.class,
                () -> CHMCache.newBuilder().maximumSize(10).cleanupInterval(Duration.ZERO));
        assertThrows(IllegalArgumentException.class,
                () -> CHMCache.newBuilder().maximumSize(10).cleanupInterval(Duration.ofMillis(-1)));
    }

    @Test
    void testExpirationStrategies() {
        // expireAfterWrite
        CHMCache<String, String> c1 = CHMCache.<String, String>newBuilder()
                .maximumSize(10).expireAfterWrite(Duration.ofSeconds(1)).build();
        // expireAfterAccess
        CHMCache<String, String> c2 = CHMCache.<String, String>newBuilder()
                .maximumSize(10).expireAfterAccess(Duration.ofSeconds(1)).build();
        // refreshAfterWrite
        CHMCache<String, String> c3 = CHMCache.<String, String>newBuilder()
                .maximumSize(10).refreshAfterWrite(Duration.ofSeconds(1)).build();
        // 自定义 expireAfter
        CHMCache<String, String> c4 = CHMCache.<String, String>newBuilder()
                .maximumSize(10).expireAfter((k, v, t) -> Duration.ofSeconds(5)).build();
        try {
            c1.put("k", "v");
            c2.put("k", "v");
            c3.put("k", "v");
            c4.put("k", "v");
        } finally {
            c1.shutdown(); c2.shutdown(); c3.shutdown(); c4.shutdown();
        }
    }

    @Test
    void testRemovalListenerInstalled() {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(2)
                .expireAfterWrite(Duration.ofSeconds(60))
                .removalListener((k, v, cause) -> calls.incrementAndGet())
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.put("c", "3"); // 触发 LRU 淘汰
            assertEquals(1, calls.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRecordStatsEnablesStats() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            cache.put("k", "v");
            cache.get("k");
            CacheStats stats = cache.stats();
            assertTrue(stats.hitCount() >= 1);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testShardedLocksToggle() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .shardedLocks(false)
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testCustomExecutor() {
        java.util.concurrent.atomic.AtomicInteger execCount = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.Executor exec = cmd -> {
            execCount.incrementAndGet();
            new Thread(cmd, "test-refresher").start();
        };
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .executor(exec)
                .build();
        try {
            cache.put("k", "v");
            cache.refresh("k", k -> "v2");
            try { Thread.sleep(100); } catch (InterruptedException ignored) {}
            assertTrue(execCount.get() >= 1);
        } finally {
            cache.shutdown();
        }
    }
}