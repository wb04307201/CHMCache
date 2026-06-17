package cn.wubo.cache.internal;

import cn.wubo.cache.CHMCache;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class CHMCacheMetricsBinderTest {

    @Test
    void testBinderRegistersSizeGauge() {
        MeterRegistry registry = new SimpleMeterRegistry();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .name("test")
                .maximumSize(100)
                .build();
        try {
            new CHMCacheMetricsBinder(cache).bindTo(registry);
            cache.put("a", "1");
            cache.put("b", "2");
            Gauge size = registry.find("chmcache.test.size").gauge();
            assertNotNull(size);
            assertEquals(2.0, size.value(), 1e-9);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testBinderRegistersCounters() {
        MeterRegistry registry = new SimpleMeterRegistry();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .name("test2")
                .maximumSize(100)
                .build();
        try {
            new CHMCacheMetricsBinder(cache).bindTo(registry);
            cache.put("a", "1");
            cache.get("a");    // hit
            cache.get("b");    // miss
            // 第一次 publish 仅设基线
            new CHMCacheMetricsBinder(cache).publish();
            Counter hit = registry.find("chmcache.test2.hit").counter();
            Counter miss = registry.find("chmcache.test2.miss").counter();
            assertNotNull(hit);
            assertNotNull(miss);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testPublishIncrementsDeltas() {
        MeterRegistry registry = new SimpleMeterRegistry();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .name("delta")
                .maximumSize(100)
                .build();
        try {
            CHMCacheMetricsBinder binder = new CHMCacheMetricsBinder(cache);
            binder.bindTo(registry);

            cache.put("a", "1");
            cache.get("a"); // hit
            cache.get("a"); // hit
            cache.get("b"); // miss

            // 第一次 publish：基线
            binder.publish();
            Counter hit = registry.find("chmcache.delta.hit").counter();
            Counter miss = registry.find("chmcache.delta.miss").counter();
            double hitBase = hit.count();
            double missBase = miss.count();
            assertEquals(0.0, hitBase, 1e-9);
            assertEquals(0.0, missBase, 1e-9);

            // 再做几次
            cache.get("a"); // hit
            cache.get("c"); // miss
            binder.publish();
            assertEquals(1.0, hit.count() - hitBase, 1e-9);
            assertEquals(1.0, miss.count() - missBase, 1e-9);
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testPublishIsNoopIfNotBound() {
        MeterRegistry registry = new SimpleMeterRegistry();
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .name("np")
                .maximumSize(10)
                .build();
        try {
            // 未 bindTo 直接 publish 不抛异常
            new CHMCacheMetricsBinder(cache).publish();
        } finally {
            cache.shutdown();
        }
    }
}