package cn.wubo.cache;

/**
 * 缓存条目被移除时的回调。监听器抛出的任何异常都会被捕获并忽略，
 * 不会影响缓存本身的正确性。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
@FunctionalInterface
public interface RemovalListener<K, V> {

    /**
     * 当条目被移除时调用。
     *
     * @param key   被移除的 key
     * @param value 被移除的 value
     * @param cause 移除原因
     */
    void onRemoval(K key, V value, RemovalCause cause);
}