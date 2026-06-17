package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class CacheStatsTest {

    @Test
    void testStatsRequireRecordStats() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .build();
        try {
            cache.put("k", "v");
            cache.get("k");
            // recordStats 关闭时 stats 仍返回对象，但延迟相关字段为 0
            CacheStats stats = cache.stats();
            assertEquals(1, stats.hitCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testStats_HitRate() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            cache.put("k", "v");
            cache.get("k");
            cache.get("k");
            cache.get("nope");
            CacheStats stats = cache.stats();
            assertEquals(2, stats.hitCount());
            assertEquals(1, stats.missCount());
            assertEquals(2.0 / 3.0, stats.hitRate(), 1e-9);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testStats_LoadFailureRate() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            try {
                cache.get("k", key -> { throw new RuntimeException("x"); });
            } catch (RuntimeException ignored) {}
            try {
                cache.get("k2", key -> "v2");
            } catch (RuntimeException ignored) {}
            CacheStats stats = cache.stats();
            assertEquals(1, stats.loadCount());
            assertEquals(1, stats.loadFailureCount());
            assertEquals(1.0, stats.loadFailureRate(), 1e-9);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testStats_TimeUnitConversion() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            cache.get("k", key -> "v");
            CacheStats stats = cache.stats();
            long ms = stats.averageGetPenalty(TimeUnit.MILLISECONDS);
            assertTrue(ms >= 0);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testStats_SizeWatermark() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100)
                .recordStats()
                .build();
        try {
            for (int i = 0; i < 50; i++) cache.put("k" + i, "v" + i);
            assertEquals(50, cache.stats().sizeWatermark());
            cache.invalidateAll();
            for (int i = 0; i < 20; i++) cache.put("k" + i, "v" + i);
            // watermark 不会回落
            assertEquals(50, cache.stats().sizeWatermark());
        } finally {
            cache.shutdown();
        }
    }
}