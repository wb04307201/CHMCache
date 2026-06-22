package cn.wubo.cache;

/**
 * 缓存统计持久化 SPI 接口。与 {@link CacheMetricsExporter} 的区别：
 * <ul>
 *   <li>{@code CacheMetricsExporter} — 一次性的指标导出（适合推送到 Prometheus / StatsD 等）</li>
 *   <li>{@code CacheStatisticsSink} — 持续累积的持久化存储（适合写入数据库 / 时序数据库 / 日志文件等）</li>
 * </ul>
 *
 * <p>实现应保证：</p>
 * <ul>
 *   <li>线程安全：多线程并发调用 {@link #record(CacheMetrics, CacheStats)} 都必须安全</li>
 *   <li>异常隔离：底层持久化系统的异常应捕获并记录，不应传播</li>
 *   <li>幂等性：同一次 record 调用失败时不应影响后续 record 调用</li>
 * </ul>
 *
 * <p>典型用法（JDBC 实现）：</p>
 * <pre>{@code
 * public class JdbcCacheStatisticsSink implements CacheStatisticsSink {
 *     private final DataSource ds;
 *     public JdbcCacheStatisticsSink(DataSource ds) { this.ds = ds; }
 *
 *     public void record(CacheMetrics metrics, CacheStats stats) {
 *         try (Connection c = ds.getConnection();
 *              PreparedStatement ps = c.prepareStatement(
 *                  "INSERT INTO cache_stats (ts, hit, miss, eviction) VALUES (?, ?, ?, ?)")) {
 *             ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
 *             ps.setLong(2, metrics.hitCount());
 *             ps.setLong(3, metrics.missCount());
 *             ps.setLong(4, metrics.evictionCount());
 *             ps.executeUpdate();
 *         } catch (SQLException e) {
 *             log.warn("record stats failed", e);
 *         }
 *     }
 * }
 *
 * // 外部调度：
 * ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
 * CHMCache<String, String> cache = ...;
 * scheduler.scheduleAtFixedRate(() -> {
 *     cache.recordStatistics(new JdbcCacheStatisticsSink(ds));
 * }, 0, 30, TimeUnit.SECONDS);
 * }</pre>
 *
 * @since 1.1.0
 */
public interface CacheStatisticsSink {

    /**
     * 记录一次统计快照。
     *
     * @param metrics 全局轻量指标（始终可用）
     * @param stats   详细指标；未启用 {@link CHMCacheBuilder#recordStats()} 时为 {@code null}
     */
    void record(CacheMetrics metrics, CacheStats stats);
}
