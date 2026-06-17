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
            assertEquals(2, readCalls.get());
        } finally {
            cache.shutdown();
        }
    }
}