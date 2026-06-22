package cn.wubo.cache;

/**
 * 缓存事件监听器。所有方法默认 no-op，实现类按需覆盖感兴趣的事件。
 *
 * <p>事件触发时机：</p>
 * <ul>
 *   <li>{@link #onHit} — {@code get} 命中未过期条目</li>
 *   <li>{@link #onMiss} — {@code get} 未命中或命中已过期条目</li>
 *   <li>{@link #onLoad} — {@link CacheLoader} 成功加载新值</li>
 *   <li>{@link #onEvict} — 因 size / weight 超限触发 LRU 淘汰</li>
 *   <li>{@link #onExpire} — TTL 到期被清理</li>
 *   <li>{@link #onReplace} — {@code put} 覆盖已有未过期条目</li>
 * </ul>
 *
 * <p>监听器抛出的异常会被 CHMCache 捕获并记录日志，不会影响缓存功能。</p>
 *
 * <p><b>实现约束：</b>监听器在缓存主路径上同步调用，必须快速且无副作用，
 * 不应包含阻塞 I/O、网络请求等。若需要异步处理，建议在 listener 内投递到独立 Executor。</p>
 *
 * <p>注册方式：通过 {@link CHMCacheBuilder#listener(CacheListener)} 在构建时设置。</p>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 * @since 1.1.0
 */
public interface CacheListener<K, V> {

    /** {@code get} 命中未过期条目时触发。 */
    default void onHit(K key, V value) {}

    /** {@code get} 未命中（含命中已过期条目）时触发。 */
    default void onMiss(K key) {}

    /** {@link CacheLoader} 成功加载新值（已写入缓存）后触发。 */
    default void onLoad(K key, V value) {}

    /** 因 size / weight 超限被 LRU 淘汰。 */
    default void onEvict(K key, V value) {}

    /** TTL 到期被清理。 */
    default void onExpire(K key, V value) {}

    /** {@code put} 覆盖已有未过期条目。 */
    default void onReplace(K key, V oldValue, V newValue) {}
}
