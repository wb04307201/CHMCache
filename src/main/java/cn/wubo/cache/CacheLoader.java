package cn.wubo.cache;

/**
 * 缓存未命中时同步加载值的策略。由 {@link CHMCache#get(Object, CacheLoader)} 使用。
 *
 * <p>loader 抛出的异常会被记录到 {@code loadFailureCount} 并向上传播。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
@FunctionalInterface
public interface CacheLoader<K, V> {

    /**
     * 加载指定 key 对应的值。返回 {@code null} 表示"该 key 无对应值"，不会写入缓存。
     */
    V load(K key) throws Exception;
}