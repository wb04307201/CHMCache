package cn.wubo.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

class CHMCacheTest {

    private CHMCache<String, String> cache;

    @BeforeEach
    void setUp() {
        cache = new CHMCache<>(100, 1000); // 100个元素，1秒TTL
    }

    @AfterEach
    void tearDown() {
        if (cache != null) {
            cache.shutdown();
        }
    }

    @Test
    void testPutAndGet() {
        cache.put("key1", "value1");
        assertEquals("value1", cache.get("key1"));
    }

    @Test
    void testGetNonExistentKey() {
        assertNull(cache.get("nonexistent"));
    }

    @Test
    void testRemove() {
        cache.put("key1", "value1");
        assertEquals("value1", cache.remove("key1"));
        assertNull(cache.get("key1"));
        assertNull(cache.remove("key1")); // 删除不存在的key
    }

    @Test
    void testExpiration() throws InterruptedException {
        cache.put("key1", "value1", 100, TimeUnit.MILLISECONDS);
        assertEquals("value1", cache.get("key1"));
        sleep(150); // 等待过期
        assertNull(cache.get("key1"));
    }

    @Test
    void testSize() {
        assertEquals(0, cache.size());
        cache.put("key1", "value1");
        assertEquals(1, cache.size());
        cache.put("key2", "value2");
        assertEquals(2, cache.size());
        cache.remove("key1");
        assertEquals(1, cache.size());
    }

    @Test
    void testLRUEviction() throws InterruptedException {
        CHMCache<Integer, Integer> smallCache = new CHMCache<>(3, 100_000);

        // 添加超过最大容量的元素
        for (int i = 0; i < 5; i++) {
            smallCache.put(i, i * 10);
        }

        sleep(10_000);

        // 验证缓存大小不超过限制
        assertTrue(smallCache.size() <= 3);

        smallCache.shutdown();
    }

    @Test
    void testAccessOrder() throws InterruptedException {
        CHMCache<Integer, Integer> smallCache = new CHMCache<>(3, 100_000);

        // 添加3个元素
        for (int i = 0; i < 3; i++) {
            smallCache.put(i, i * 10);
        }

        // 访问第一个元素，使其变为最近使用
        assertEquals(Integer.valueOf(0), smallCache.get(0));

        // 添加第4个元素，应该淘汰最久未使用的元素（key=1）
        smallCache.put(3, 30);

        sleep(10_000);

        assertNotNull(smallCache.get(0)); // key=0应该还在（因为刚访问过）
        assertNull(smallCache.get(1));   // key=1应该被淘汰
        assertNotNull(smallCache.get(2)); // key=2应该还在
        assertNotNull(smallCache.get(3)); // key=3应该还在

        smallCache.shutdown();
    }

    @Test
    void testMetrics() throws InterruptedException {
        // 初始状态
        MonitorMetrics initialMetrics = cache.getMetrics();
        assertEquals(0, initialMetrics.getHitCount());
        assertEquals(0, initialMetrics.getMissCount());
        assertEquals(0, initialMetrics.getEvictionCount());
        assertEquals(0, initialMetrics.getCurrentSize());

        // 测试命中和未命中
        cache.put("key1", "value1");
        cache.get("key1"); // 命中
        cache.get("key2"); // 未命中

        MonitorMetrics metrics1 = cache.getMetrics();
        assertEquals(1, metrics1.getHitCount());
        assertEquals(1, metrics1.getMissCount());
        assertEquals(0, metrics1.getEvictionCount());
        assertEquals(1, metrics1.getCurrentSize());

        // 测试过期
        cache.put("key2", "value2", 10, TimeUnit.MILLISECONDS);
        sleep(50); // 等待过期
        cache.get("key2"); // 应该未命中

        MonitorMetrics metrics2 = cache.getMetrics();
        assertEquals(1, metrics2.getHitCount());
        assertEquals(2, metrics2.getMissCount());
        assertEquals(0, metrics2.getEvictionCount());
        assertEquals(1, metrics2.getCurrentSize());
    }

    @Test
    void testCleanup() throws InterruptedException {
        cache.put("key1", "value1", 10, TimeUnit.MILLISECONDS);
        cache.put("key2", "value2");

        assertEquals(2, cache.size());

        sleep(50); // 等待key1过期
        cache.cleanup();  // 手动清理

        assertEquals(1, cache.size());
        assertNull(cache.get("key1"));
        assertEquals("value2", cache.get("key2"));
    }

    @Test
    void testDefaultConstructor() {
        CHMCache<String, String> defaultCache = new CHMCache<>();
        defaultCache.put("key", "value");
        assertEquals("value", defaultCache.get("key"));
        defaultCache.shutdown();
    }
}
