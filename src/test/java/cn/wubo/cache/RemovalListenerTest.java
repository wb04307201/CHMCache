package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RemovalListenerTest {

    @Test
    void testExplicitInvalidateFiresExplicit() {
        List<RemovalCause> causes = new ArrayList<>();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofSeconds(60))
                .removalListener((k, v, cause) -> causes.add(cause))
                .build();
        try {
            cache.put("k", "v");
            cache.invalidate("k");
            assertEquals(1, causes.size());
            assertEquals(RemovalCause.EXPLICIT, causes.get(0));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testReplacementFiresReplaced() {
        List<RemovalCause> causes = new ArrayList<>();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofSeconds(60))
                .removalListener((k, v, cause) -> causes.add(cause))
                .build();
        try {
            cache.put("k", "v1");
            cache.put("k", "v2");
            assertEquals(1, causes.size());
            assertEquals(RemovalCause.REPLACED, causes.get(0));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testInvalidateAllFiresExplicitForAll() {
        List<RemovalCause> causes = new ArrayList<>();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofSeconds(60))
                .removalListener((k, v, cause) -> causes.add(cause))
                .build();
        try {
            cache.put("a", "1");
            cache.put("b", "2");
            cache.invalidateAll();
            assertEquals(2, causes.size());
            causes.forEach(c -> assertEquals(RemovalCause.EXPLICIT, c));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testListenerExceptionDoesNotBreakCache() {
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .expireAfterWrite(Duration.ofSeconds(60))
                .removalListener((k, v, cause) -> { throw new RuntimeException("boom"); })
                .build();
        try {
            cache.put("k", "v");
            assertEquals("v", cache.get("k"));
            cache.invalidate("k");
            assertNull(cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }
}