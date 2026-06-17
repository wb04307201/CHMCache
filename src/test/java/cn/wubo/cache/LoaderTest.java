package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class LoaderTest {

    @Test
    void testGetWithLoader_HitsCacheNoLoad() {
        AtomicInteger calls = new AtomicInteger(0);
        CacheLoader<String, String> loader = k -> {
            calls.incrementAndGet();
            return "loaded-" + k;
        };
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("k", "v");
            String v = cache.get("k", loader);
            assertEquals("v", v);
            assertEquals(0, calls.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testGetWithLoader_LoadsOnMiss() {
        AtomicInteger calls = new AtomicInteger(0);
        CacheLoader<String, String> loader = k -> {
            calls.incrementAndGet();
            return "loaded-" + k;
        };
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            String v = cache.get("k", loader);
            assertEquals("loaded-k", v);
            assertEquals(1, calls.get());
            // 再次获取应从缓存
            String v2 = cache.get("k", loader);
            assertEquals("loaded-k", v2);
            assertEquals(1, calls.get());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testGetWithLoader_NullResult() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            String v = cache.get("k", k -> null);
            assertNull(v);
            assertEquals(0, cache.size());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testGetWithLoader_Exception() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            RuntimeException boom = new RuntimeException("nope");
            assertThrows(RuntimeException.class, () -> cache.get("k", k -> { throw boom; }));
            assertEquals(1, cache.stats().loadFailureCount());
            assertEquals(0, cache.stats().loadCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefresh_UpdatesValueAsynchronously() throws InterruptedException {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("k", "v1");
            cache.refresh("k", k -> "v2");
            // 等待后台任务完成
            long deadline = System.currentTimeMillis() + 2_000;
            while (!"v2".equals(cache.get("k")) && System.currentTimeMillis() < deadline) {
                Thread.sleep(20);
            }
            assertEquals("v2", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefresh_FailureDoesNotBreakCache() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("k", "v");
            // 不抛异常，吞掉
            cache.refresh("k", k -> { throw new RuntimeException("nope"); });
            // v 仍在
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefresh_LoaderReturnsNull() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("k", "v");
            cache.refresh("k", k -> null);
            // 旧值仍在（refresh 加载到 null 不替换）
            try { Thread.sleep(50); } catch (InterruptedException ignored) {}
            assertEquals("v", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testLoadCountReflectsCacheLoaderUsage() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .recordStats()
                .build();
        try {
            cache.get("k", key -> "v");
            cache.get("k2", key -> "v2");
            assertEquals(2, cache.stats().loadCount());
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testGetAll_HitsAndMisses() {
        // D18 回归测试
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");

            java.util.Set<String> requested = new java.util.HashSet<>(java.util.Arrays.asList("a", "b", "c", "d"));
            java.util.Map<String, String> result = cache.getAll(requested, missing -> {
                java.util.Map<String, String> loaded = new java.util.HashMap<>();
                loaded.put("c", "3");
                loaded.put("d", "4");
                return loaded;
            });
            assertEquals(4, result.size());
            assertEquals("1", result.get("a"));
            assertEquals("2", result.get("b"));
            assertEquals("3", result.get("c"));
            assertEquals("4", result.get("d"));
            // 缺失的 key 应已回填到缓存
            assertEquals("3", cache.get("c"));
            assertEquals("4", cache.get("d"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testGetAll_LoaderReturnsNull() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10).build();
        try {
            cache.put("a", "1");
            java.util.Map<String, String> result = cache.getAll(
                    java.util.Set.of("a", "b"),
                    missing -> null);
            assertEquals(1, result.size());
            assertEquals("1", result.get("a"));
        } finally {
            cache.shutdown();
        }
    }
}