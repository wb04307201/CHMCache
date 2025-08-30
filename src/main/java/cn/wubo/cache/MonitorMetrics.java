package cn.wubo.cache;

public class MonitorMetrics {

    private final long hitCount;
    private final long missCount;
    private final long evictionCount;
    private final long cleanupTimeNanos;
    private final int currentSize;

    public MonitorMetrics(long hitCount, long missCount, long evictionCount, long cleanupTimeNanos, int currentSize) {
        this.hitCount = hitCount;
        this.missCount = missCount;
        this.evictionCount = evictionCount;
        this.cleanupTimeNanos = cleanupTimeNanos;
        this.currentSize = currentSize;
    }

    public long getHitCount() {
        return hitCount;
    }

    public long getMissCount() {
        return missCount;
    }

    public long getEvictionCount() {
        return evictionCount;
    }

    public long getCleanupTimeNanos() {
        return cleanupTimeNanos;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    // 获取缓存命中率
    public double getHitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0 : (double) hitCount / total;
    }

    // 获取平均清理耗时（毫秒）
    public double getAverageCleanupTimeMillis() {
        long count = cleanupTimeNanos;
        return count == 0 ? 0 : (double) count / 1_000_000;
    }
}
