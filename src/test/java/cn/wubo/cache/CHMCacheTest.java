package cn.wubo.cache;

import junit.framework.TestCase;

public class CHMCacheTest extends TestCase {

    public void testApp() throws InterruptedException {
        // 创建缓存：最大1000项，默认TTL 60秒
        CHMCache<String, String> cache = new CHMCache<>(1000, 60_000);

        // 添加缓存项
        cache.put("key1", "value1");
        cache.put("key2", "value2", 30_000); // 自定义TTL 30秒

        // 获取缓存项
        String value = cache.get("key1");
        System.out.println("Got value: " + value);

        // 监控输出
        System.out.println("Hit rate: " + cache.getHitRate());
        System.out.println("Current size: " + cache.getCurrentSize());

        // 等待并观察过期
        Thread.sleep(35_000);
        System.out.println("After 35s, key2 exists: " + (cache.get("key2") != null));

        // 关闭缓存
        cache.shutdown();
    }
}
