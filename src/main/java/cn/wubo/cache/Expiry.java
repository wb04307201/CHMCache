package cn.wubo.cache;

import java.time.Duration;

/**
 * 自定义过期策略接口。允许用户在每次创建/更新/读取条目时动态计算该条目的
 * "存活时长"，适用于"按业务属性动态过期"等场景。
 *
 * <p>若同时配置了 {@link Expirations}（如 expireAfterWrite），此接口的返回值优先。
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
@FunctionalInterface
public interface Expiry<K, V> {

    /**
     * 返回条目在指定事件发生后还可存活多久。
     *
     * @param key          当前 key
     * @param value        当前 value
     * @param currentTime  当前 {@link System#nanoTime()}（单调时钟）
     * @return 该条目到期所需的时长。返回 {@code Duration.ZERO} 表示立即过期。
     */
    Duration expireAfterCreate(K key, V value, long currentTime);

    /**
     * 当条目被读取命中时调用。默认行为：返回 {@link Expiry#expireAfterCreate(Object, Object, long)}。
     */
    default Duration expireAfterUpdate(K key, V value, long currentTime) {
        return expireAfterCreate(key, value, currentTime);
    }

    /**
     * 当条目被读取命中时调用。默认行为：返回 {@link Expiry#expireAfterUpdate(Object, Object, long)}。
     */
    default Duration expireAfterRead(K key, V value, long currentTime) {
        return expireAfterUpdate(key, value, currentTime);
    }
}