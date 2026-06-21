package cn.wubo.cache;

import java.util.function.LongSupplier;

/**
 * 缓存值包装。
 *
 * <p>采用可注入的单调时钟（默认 {@link System#nanoTime()}）计算 TTL，避免系统时钟回拨
 * （NTP 校时）导致过期判断异常。每个实例携带唯一 {@link #token}，与 {@link DelayedItem}
 * 中的 token 配对用于过期回收时的原子校验。
 *
 * <p>{@link #accessTimeNanos} 用于 {@link Expirations#afterAccess}（滑动 TTL）：
 * get 命中时刷新该字段，下次判断 isExpired 时基于 accessTimeNanos。
 *
 * @param <V> 值类型
 */
final class CacheValue<V> {

    /** 实际缓存值。 */
    final V value;

    /** 创建时刻（来自注入的单调时钟）。 */
    final long createTimeNanos;

    /** 当前过期时长（纳秒，可能因 expireAfterRead 刷新）。 */
    final long ttlNanos;

    /** 最近一次访问时刻（来自注入的单调时钟），滑动 TTL 用；非滑动时等于 createTimeNanos。 */
    final long accessTimeNanos;

    /** 唯一标识，与 {@link DelayedItem#token} 配对用于原子校验。 */
    final long token;

    /** 此条目累计占用权重（用于 weight-based 淘汰）。 */
    final int weight;

    /** 单调时钟源，用于过期判断。 */
    private final LongSupplier nanoTimeSource;

    CacheValue(V value, long ttlNanos, long token, int weight, LongSupplier nanoTimeSource) {
        this.value = value;
        this.nanoTimeSource = nanoTimeSource;
        this.createTimeNanos = nanoTimeSource.getAsLong();
        this.ttlNanos = ttlNanos;
        this.accessTimeNanos = this.createTimeNanos;
        this.token = token;
        this.weight = weight;
    }

    /**
     * 带显式访问时刻的构造器。用于滑动 TTL 命中时重建实例（避免再次调用时钟源）。
     */
    CacheValue(V value, long ttlNanos, long token, int weight,
               long createTimeNanos, long accessTimeNanos, LongSupplier nanoTimeSource) {
        this.value = value;
        this.nanoTimeSource = nanoTimeSource;
        this.createTimeNanos = createTimeNanos;
        this.ttlNanos = ttlNanos;
        this.accessTimeNanos = accessTimeNanos;
        this.token = token;
        this.weight = weight;
    }

    /**
     * 是否已过期。基于注入的单调时钟。
     */
    boolean isExpired() {
        return nanoTimeSource.getAsLong() - accessTimeNanos > ttlNanos;
    }
}