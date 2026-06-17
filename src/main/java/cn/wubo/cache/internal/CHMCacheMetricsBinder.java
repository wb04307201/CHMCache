package cn.wubo.cache.internal;

import cn.wubo.cache.CHMCache;
import cn.wubo.cache.CacheMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.atomic.AtomicReference;

/**
 * 将 CHMCache 的指标绑定到 Micrometer {@link MeterRegistry}。
 *
 * <p>使用示例：
 * <pre>{@code
 * SimpleMeterRegistry registry = new SimpleMeterRegistry();
 * new CHMCacheMetricsBinder(cache).bindTo(registry);
 * }</pre>
 *
 * <p>注册指标：
 * <ul>
 *   <li>{@code chmcache.<name>.size} — gauge</li>
 *   <li>{@code chmcache.<name>.hit} — counter</li>
 *   <li>{@code chmcache.<name>.miss} — counter</li>
 *   <li>{@code chmcache.<name>.eviction} — counter</li>
 *   <li>{@code chmcache.<name>.expiration} — counter</li>
 * </ul>
 */
public final class CHMCacheMetricsBinder {

    private final CHMCache<?, ?> cache;
    private final AtomicReference<CacheMetrics> lastSnapshot = new AtomicReference<>();
    private Counter hitCounter;
    private Counter missCounter;
    private Counter evictionCounter;
    private Counter expirationCounter;

    public CHMCacheMetricsBinder(CHMCache<?, ?> cache) {
        this.cache = cache;
    }

    public void bindTo(MeterRegistry registry) {
        String name = "chmcache." + cache.getName();

        Gauge.builder(name + ".size", cache, CHMCache::size)
                .description("Current cache size")
                .register(registry);

        // 计数器用差值上报：每次 bind 时记录当前快照的差值
        hitCounter = Counter.builder(name + ".hit")
                .description("Cache hit count")
                .register(registry);
        missCounter = Counter.builder(name + ".miss")
                .description("Cache miss count")
                .register(registry);
        evictionCounter = Counter.builder(name + ".eviction")
                .description("Cache eviction count (LRU)")
                .register(registry);
        expirationCounter = Counter.builder(name + ".expiration")
                .description("Cache expiration count")
                .register(registry);

        // 延迟 timer（仅 recordStats 时有意义）
        Timer.builder(name + ".get.penalty")
                .description("Get latency")
                .register(registry);
    }

    /**
     * 由用户在定期任务中调用，将指标差值推送到 Counter。
     */
    public void publish() {
        if (hitCounter == null) return;
        CacheMetrics now = cache.metrics();
        CacheMetrics prev = lastSnapshot.getAndSet(now);
        if (prev == null) {
            // 第一次调用：把当前值作为基准，避免一次性暴涨
            return;
        }
        hitCounter.increment(now.hitCount() - prev.hitCount());
        missCounter.increment(now.missCount() - prev.missCount());
        evictionCounter.increment(now.evictionCount() - prev.evictionCount());
        expirationCounter.increment(now.expirationCount() - prev.expirationCount());
    }
}