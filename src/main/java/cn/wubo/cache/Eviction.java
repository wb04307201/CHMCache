package cn.wubo.cache;

/**
 * 容量淘汰策略接口。根据当前条目数和权重判断是否需要淘汰。
 *
 * <p>由 {@link Evictions} 提供两种预设：
 * <ul>
 *   <li>{@link Evictions#sizeBased()} — 按条目数淘汰</li>
 *   <li>{@link Evictions#weightBased()} — 按累计权重淘汰</li>
 * </ul>
 */
public interface Eviction {

    /**
     * 是否已达到淘汰阈值。
     *
     * @param currentSize    cacheMap 当前条目数
     * @param maxSize        maximumSize 配置
     * @param currentWeight  cacheMap 当前累计权重
     * @param maxWeight      maximumWeight 配置（未配置时为 -1）
     */
    boolean shouldEvict(long currentSize, long maxSize, long currentWeight, long maxWeight);

    /**
     * 收集统计指标用的当前总量（size 或 weight）。
     */
    long total(long currentSize, long currentWeight);

    /**
     * 该 Eviction 对应的上限值（size 或 weight）。
     */
    long limit(long maxSize, long maxWeight);

    /**
     * 释放一个条目时连带减少对应统计。
     */
    void onRemoval(long[] sizeAndWeight);
}