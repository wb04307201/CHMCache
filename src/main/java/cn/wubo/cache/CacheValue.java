package cn.wubo.cache;

/**
 * 缓存值包装。
 *
 * <p>采用 {@link System#nanoTime()} 单调时钟计算 TTL，避免系统时钟回拨（NTP 校时）导致
 * 过期判断异常。每个实例携带唯一 {@link #token}，与 {@link DelayedItem} 中的 token
 * 配对用于过期回收时的原子校验。
 *
 * <p>{@link #accessTimeNanos} 用于 {@link Expirations#afterAccess}（滑动 TTL）：
 * get 命中时刷新该字段，下次判断 isExpired 时基于 accessTimeNanos。
 *
 * @param <V> 值类型
 */
final class CacheValue<V> {

    /** 实际缓存值。 */
    final V value;

    /** 创建时刻（{@link System#nanoTime()}）。 */
    final long createTimeNanos;

    /** 当前过期时长（纳秒，可能因 expireAfterRead 刷新）。 */
    final long ttlNanos;

    /** 最近一次访问时刻（{@link System#nanoTime()}），滑动 TTL 用；非滑动时等于 createTimeNanos。 */
    final long accessTimeNanos;

    /** 唯一标识，与 {@link DelayedItem#token} 配对用于原子校验。 */
    final long token;

    /** 此条目累计占用权重（用于 weight-based 淘汰）。 */
    final int weight;

    CacheValue(V value, long ttlNanos, long token, int weight) {
        this.value = value;
        this.createTimeNanos = System.nanoTime();
        this.ttlNanos = ttlNanos;
        this.accessTimeNanos = this.createTimeNanos;
        this.token = token;
        this.weight = weight;
    }

    /**
     * 是否已过期。基于单调时钟。
     */
    boolean isExpired() {
        return System.nanoTime() - accessTimeNanos > ttlNanos;
    }
}