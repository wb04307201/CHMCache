package cn.wubo.cache;

/**
 * 单个 key 的统计指标。
 * 由 {@link CHMCache#stats(String)} / {@link CHMCache#allStats()} 返回，
 * 仅在 {@link CHMCacheBuilder#enablePerKeyMetrics(boolean)} 启用时收集。
 *
 * <p>不可变 record。
 *
 * @since 1.1.0
 */
public record KeyStatistics(
        /** 缓存 key。 */
        Object key,
        /** 累计的 get 命中次数。 */
        long hitCount,
        /** 累计的 get 未命中次数（含 loader 调用）。 */
        long missCount,
        /** 累计的 put 次数（含替换）。 */
        long putCount,
        /** 累计的淘汰次数。 */
        long evictionCount,
        /** 累计的过期次数。 */
        long expirationCount,
        /** 最近一次 put 的 epoch 毫秒时间戳；0 表示从未 put。 */
        long lastPutEpochMs,
        /** 最近一次 get 命中的 epoch 毫秒时间戳；0 表示从未命中。 */
        long lastHitEpochMs
) {

    /** @return 命中率，范围 {@code [0.0, 1.0]}；{@link #hitCount() + #missCount()} 为 0 时返回 0.0 */
    public double hitRate() {
        long total = hitCount + missCount;
        return total == 0 ? 0.0 : (double) hitCount / total;
    }
}
