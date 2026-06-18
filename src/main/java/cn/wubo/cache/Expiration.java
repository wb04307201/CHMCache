package cn.wubo.cache;

import java.time.Duration;

/**
 * 过期策略接口。定义在条目创建/更新/读取时应使用的剩余 TTL。
 *
 * <p>由 {@link Expirations} 提供三类预设实现：
 * <ul>
 *   <li>{@link Expirations#afterWrite(Duration)} — 写后固定 TTL</li>
 *   <li>{@link Expirations#afterAccess(Duration)} — 滑动 TTL（get 命中时刷新）</li>
 *   <li>{@link Expirations#refreshAfterWrite(Duration)} — 写后异步刷新</li>
 * </ul>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public interface Expiration<K, V> {

    /**
     * 条目创建时应使用的 TTL。返回 {@link Duration#ZERO} 表示永不过期（不推荐）。
     */
    Duration expireAfterCreate(K key, V value, long currentTime);

    /**
     * 条目被覆盖（同一 key 再 put）时应使用的 TTL。默认等于 {@link #expireAfterCreate}。
     */
    default Duration expireAfterUpdate(K key, V value, long currentTime) {
        return expireAfterCreate(key, value, currentTime);
    }

    /**
     * 条目被读取命中时应使用的 TTL。默认等于 {@link #expireAfterUpdate}。
     */
    default Duration expireAfterRead(K key, V value, long currentTime) {
        return expireAfterUpdate(key, value, currentTime);
    }
}