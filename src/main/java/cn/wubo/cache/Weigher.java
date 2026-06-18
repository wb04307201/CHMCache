package cn.wubo.cache;

/**
 * 计算每个缓存条目"权重"的策略。当 {@link CHMCacheBuilder#maximumWeight(long)}
 * 配置启用后，将按累加权重决定是否触发淘汰。
 *
 * <p>默认实现 {@link Weigher#singletonWeigher()} 永远返回 1，即等价于按条目数限流。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
@FunctionalInterface
public interface Weigher<K, V> {

    /**
     * 返回指定条目占用的权重。必须返回正整数。
     */
    int weigh(K key, V value);

    /**
     * 返回一个所有条目权重均为 1 的 Weigher。
     */
    static <K, V> Weigher<K, V> singletonWeigher() {
        return (k, v) -> 1;
    }
}