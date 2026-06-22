package cn.wubo.cache;

/**
 * 缓存指标导出 SPI。实现类负责把 CHMCache 的指标序列化为目标格式
 * （JSON / Prometheus / StatsD 等）。
 *
 * <p>调用方通过 {@link CHMCache#exportMetrics(CacheMetricsExporter)} 主动触发导出 —
 * CHMCache 当前不内置周期性导出线程，如有需要请在外部自行调度。
 * 若需要持续累积的持久化存储，请使用 {@link CacheStatisticsSink} 并自行管理调度。</p>
 *
 * @see JsonCacheMetricsExporter
 * @see CacheStatisticsSink
 * @since 1.1.0
 */
public interface CacheMetricsExporter {

    /**
     * 导出轻量指标快照。{@code metrics} 始终可用。
     *
     * @param metrics 全局轻量指标（命中率、当前 size、淘汰数等）
     */
    void exportMetrics(CacheMetrics metrics);

    /**
     * 导出详细指标快照。仅当 {@link CHMCacheBuilder#recordStats()} 启用时
     * {@code stats} 非 null。
     *
     * @param stats 详细指标（延迟分位、load 失败率等）；未启用时为 {@code null}
     */
    default void exportStats(CacheStats stats) {
        // 默认忽略；实现方可按需覆盖
    }
}
