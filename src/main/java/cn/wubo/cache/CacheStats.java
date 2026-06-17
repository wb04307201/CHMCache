package cn.wubo.cache;

import java.util.concurrent.TimeUnit;

/**
 * 缓存运行统计快照。仅当 {@link CHMCacheBuilder#recordStats()} 调用后，
 * 延迟、热点 Key 等开销较高的指标才会被记录。
 *
 * <p>不可变对象。
 */
public final class CacheStats {

    private final long hitCount;
    private final long missCount;
    private final long loadCount;
    private final long loadFailureCount;
    private final long evictionCount;
    private final long expirationCount;
    private final long totalLoadTimeNanos;
    private final long averageGetPenaltyNanos;
    private final long minGetPenaltyNanos;
    private final long maxGetPenaltyNanos;
    private final long sizeWatermark;

    public CacheStats(long hitCount,
                      long missCount,
                      long loadCount,
                      long loadFailureCount,
                      long evictionCount,
                      long expirationCount,
                      long totalLoadTimeNanos,
                      long averageGetPenaltyNanos,
                      long minGetPenaltyNanos,
                      long maxGetPenaltyNanos,
                      long sizeWatermark) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.loadCount = loadCount;
        this.loadFailureCount = loadFailureCount;
        this.evictionCount = evictionCount;
        this.expirationCount = expirationCount;
        this.totalLoadTimeNanos = totalLoadTimeNanos;
        this.averageGetPenaltyNanos = averageGetPenaltyNanos;
        this.minGetPenaltyNanos = minGetPenaltyNanos;
        this.maxGetPenaltyNanos = maxGetPenaltyNanos;
        this.sizeWatermark = sizeWatermark;
    }

    public long hitCount() { return hitCount; }
    public long missCount() { return missCount; }
    public long loadCount() { return loadCount; }
    public long loadFailureCount() { return loadFailureCount; }
    public long evictionCount() { return evictionCount; }
    public long expirationCount() { return expirationCount; }
    public long totalLoadTime(TimeUnit unit) {
        return unit.convert(totalLoadTimeNanos, TimeUnit.NANOSECONDS);
    }
    public long averageGetPenalty(TimeUnit unit) {
        return unit.convert(averageGetPenaltyNanos, TimeUnit.NANOSECONDS);
    }
    public long minGetPenalty(TimeUnit unit) {
        return unit.convert(minGetPenaltyNanos, TimeUnit.NANOSECONDS);
    }
    public long maxGetPenalty(TimeUnit unit) {
        return unit.convert(maxGetPenaltyNanos, TimeUnit.NANOSECONDS);
    }
    public long sizeWatermark() { return sizeWatermark; }

    /**
     * 命中率。
     */
    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / total;
    }

    /**
     * miss → load 成功率（不含返回 null 的情况）。
     */
    public double loadFailureRate() {
        return loadCount == 0 ? 0.0 : (double) loadFailureCount / loadCount;
    }
}