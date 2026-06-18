package cn.wubo.cache;

/**
 * 缓存指标的"轻量视图"。与 {@link CacheStats} 区别：本类始终可用，无需 recordStats；
 * 主要反映基础计数器。详细延迟分布请调用 {@link CHMCache#stats()}。
 */
public final class CacheMetrics {

    private final long currentSize;
    private final long hitCount;
    private final long missCount;
    private final long evictionCount;
    private final long expirationCount;
    private final long cleanupTimeNanos;
    private final long cleanupRunCount;

    public CacheMetrics(long currentSize,
                        long hitCount,
                        long missCount,
                        long evictionCount,
                        long expirationCount,
                        long cleanupTimeNanos,
                        long cleanupRunCount) {
        this.currentSize = currentSize;
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.evictionCount = evictionCount;
        this.expirationCount = expirationCount;
        this.cleanupTimeNanos = cleanupTimeNanos;
        this.cleanupRunCount = cleanupRunCount;
    }

    public long currentSize() { return currentSize; }
    public long hitCount() { return hitCount; }
    public long missCount() { return missCount; }
    public long evictionCount() { return evictionCount; }
    public long expirationCount() { return expirationCount; }
    public long cleanupTimeNanos() { return cleanupTimeNanos; }
    public long cleanupRunCount() { return cleanupRunCount; }

    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / total;
    }
}