package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class EvictionStrategyTest {

    @Test
    void testSizeBased_EvictsAtMaxSize() {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(3)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
        try {
            for (int i = 0; i < 10; i++) cache.put(i, i);
            assertTrue(cache.size() <= 3);
            assertEquals(7, cache.metrics().evictionCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testWeightBased_EvictsAtMaxWeight() {
        CHMCache<String, byte[]> cache = CHMCache.<String, byte[]>newBuilder()
                .maximumWeight(100)
                .weigher((k, v) -> v.length)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
        try {
            // 50 + 50 = 100, 加 30 应触发淘汰
            cache.put("a", new byte[50]);
            cache.put("b", new byte[50]);
            cache.put("c", new byte[30]);
            assertTrue(cache.weightedSize() <= 100);
            assertTrue(cache.metrics().evictionCount() >= 1);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testDefaultWeigherIsSingleton() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumWeight(5)
                .expireAfterWrite(Duration.ofSeconds(60))
                .build();
        try {
            for (int i = 0; i < 10; i++) cache.put("k" + i, "v");
            assertTrue(cache.size() <= 5);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testEvictionCause() {
        java.util.concurrent.atomic.AtomicReference<RemovalCause> lastCause = new java.util.concurrent.atomic.AtomicReference<>();
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2)
                .expireAfterWrite(Duration.ofSeconds(60))
                .removalListener((k, v, cause) -> lastCause.set(cause))
                .build();
        try {
            cache.put(1, 1);
            cache.put(2, 2);
            cache.put(3, 3);
            assertEquals(RemovalCause.SIZE, lastCause.get());
        } finally {
            cache.shutdown();
        }
    }
}