package cn.wubo.cache;

/**
 * 异步刷新缓存值。由 {@link CHMCache#refresh(Object, RefreshLoader)} 使用。
 *
 * <p>调用方立即拿到旧值（可能为 {@code null}），后台异步加载完成后通过 {@code put} 替换。
 * 整个刷新过程中读端不会被阻塞。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
@FunctionalInterface
public interface RefreshLoader<K, V> {

    /**
     * 重新加载指定 key 对应的值。
     */
    V load(K key) throws Exception;
}