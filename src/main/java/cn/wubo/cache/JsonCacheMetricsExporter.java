package cn.wubo.cache;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 默认的 JSON 指标导出器，使用 {@link Appendable} 作为输出目的地。
 * 零三方依赖 — 不使用 Jackson/Gson，手写序列化以保持库的零依赖特性。
 *
 * @see CacheMetricsExporter
 * @since 1.1.0
 */
public class JsonCacheMetricsExporter implements CacheMetricsExporter {

    private final Appendable out;

    /** 默认输出到 {@link System#out}。 */
    public JsonCacheMetricsExporter() {
        this(System.out);
    }

    /** 指定输出目的地，通常为 {@link StringBuilder}（测试）或 {@link java.io.PrintWriter}（文件）。 */
    public JsonCacheMetricsExporter(Appendable out) {
        this.out = out;
    }

    @Override
    public void exportMetrics(CacheMetrics m) {
        try {
            out.append("{\n");
            appendMetrics(out, m);
            out.append("\n}");
        } catch (IOException e) {
            throw new RuntimeException("JsonCacheMetricsExporter failed", e);
        }
    }

    @Override
    public void exportStats(CacheStats s) {
        if (s == null) return;
        try {
            out.append("{\n");
            appendStats(out, s);
            out.append("\n}");
        } catch (IOException e) {
            throw new RuntimeException("JsonCacheMetricsExporter failed", e);
        }
    }

    private static void appendMetrics(Appendable o, CacheMetrics m) throws IOException {
        o.append("  \"currentSize\":").append(String.valueOf(m.currentSize()));
        appendField(o, "hitCount", m.hitCount());
        appendField(o, "missCount", m.missCount());
        appendField(o, "evictionCount", m.evictionCount());
        appendField(o, "expirationCount", m.expirationCount());
        appendField(o, "cleanupTimeNanos", m.cleanupTimeNanos());
        appendField(o, "cleanupRunCount", m.cleanupRunCount());
        appendField(o, "hitRate", m.hitRate());
    }

    private static void appendStats(Appendable o, CacheStats s) throws IOException {
        o.append("  \"hitCount\":").append(String.valueOf(s.hitCount()));
        appendField(o, "missCount", s.missCount());
        appendField(o, "loadCount", s.loadCount());
        appendField(o, "loadFailureCount", s.loadFailureCount());
        appendField(o, "evictionCount", s.evictionCount());
        appendField(o, "expirationCount", s.expirationCount());
        appendField(o, "totalLoadTimeNanos", s.totalLoadTime(TimeUnit.NANOSECONDS));
        appendField(o, "averageGetPenaltyNanos", s.averageGetPenalty(TimeUnit.NANOSECONDS));
        appendField(o, "minGetPenaltyNanos", s.minGetPenalty(TimeUnit.NANOSECONDS));
        appendField(o, "maxGetPenaltyNanos", s.maxGetPenalty(TimeUnit.NANOSECONDS));
        appendField(o, "sizeWatermark", s.sizeWatermark());
        appendField(o, "hitRate", s.hitRate());
        appendField(o, "loadFailureRate", s.loadFailureRate());
    }

    private static void appendField(Appendable o, String name, long value) throws IOException {
        o.append(",\n    \"").append(name).append("\":").append(String.valueOf(value));
    }

    private static void appendField(Appendable o, String name, double value) throws IOException {
        o.append(",\n    \"").append(name).append("\":").append(String.valueOf(value));
    }

    /**
     * 辅助方法，用于测试。返回序列化后的 JSON 字符串。
     * 行为等价于 {@code new JsonCacheMetricsExporter(sb).exportMetrics(m)} 然后
     * {@code sb.toString()}。仅作测试便利，不建议在生产代码中使用。
     */
    public static String metricsToString(CacheMetrics m) {
        StringBuilder sb = new StringBuilder();
        new JsonCacheMetricsExporter(sb).exportMetrics(m);
        return sb.toString();
    }

    /**
     * 辅助方法，用于测试。返回序列化后的 JSON 字符串。
     */
    public static String statsToString(CacheStats s) {
        if (s == null) return "null";
        StringBuilder sb = new StringBuilder();
        new JsonCacheMetricsExporter(sb).exportStats(s);
        return sb.toString();
    }
}
