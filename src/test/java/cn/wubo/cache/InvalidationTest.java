package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class InvalidationTest {

    @Test
    void testInvalidate() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.invalidate("k"));
            assertNull(cache.invalidate("k"));
            assertEquals(0, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testInvalidateIf() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(100).build();
        try {
            for (int i = 0; i < 20; i++) {
                cache.put("k" + i, "v" + i);
            }
            cache.invalidateIf(k -> ((String) k).hashCode() % 2 == 0);
            // 断言：剩余数量小于 20（具体取决于 hash 分布）
            assertTrue(cache.size() < 20);
            assertTrue(cache.size() > 0);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testInvalidateIfPredicateCannotBeNull() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            assertThrows(NullPointerException.class, () -> cache.invalidateIf(null));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testInvalidateAll() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.invalidateAll();
            assertEquals(0, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testInvalidateReturnsOldValue() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("k", "v");
            String old = cache.invalidate("k");
            assertEquals("v", old);
        } finally {
            cache.shutdown();
        }
    }
}