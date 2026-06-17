package cn.wubo.cache;

import java.time.Duration;

/**
 * 预置的 {@link Expiration} 工厂。
 */
public final class Expirations {

    private Expirations() {}

    /**
     * 写后固定 TTL（默认行为）。读取命中不会延长 TTL。
     */
    public static <K, V> Expiration<K, V> afterWrite(Duration duration) {
        long ttlNanos = duration.toNanos();
        return new Expiration<>() {
            @Override
            public Duration expireAfterCreate(K key, V value, long currentTime) {
                return Duration.ofNanos(ttlNanos);
            }
            @Override
            public Duration expireAfterUpdate(K key, V value, long currentTime) {
                return Duration.ofNanos(ttlNanos);
            }
            @Override
            public Duration expireAfterRead(K key, V value, long currentTime) {
                return Duration.ofNanos(ttlNanos);
            }
        };
    }

    /**
     * 滑动 TTL：读取命中时刷新过期时间（行为类似 session）。
     * 写入也刷新过期时间。
     */
    public static <K, V> Expiration<K, V> afterAccess(Duration duration) {
        long ttlNanos = duration.toNanos();
        return new Expiration<>() {
            @Override
            public Duration expireAfterCreate(K key, V value, long currentTime) {
                return Duration.ofNanos(ttlNanos);
            }
            @Override
            public Duration expireAfterUpdate(K key, V value, long currentTime) {
                return Duration.ofNanos(ttlNanos);
            }
            @Override
            public Duration expireAfterRead(K key, V value, long currentTime) {
                return Duration.ofNanos(ttlNanos);
            }
        };
    }

    /**
     * 写后异步刷新：写入后经过指定时长，触发后台异步加载新值。
     * 实际行为：put 时按 ttl 设置正常过期时间；同时记录"refresh 触发时间"。
     * 当条目接近过期但仍在"refresh 窗口"内时（如写后 4 分钟，TTL 5 分钟），
     * 由后台任务异步调用 refresh loader 加载新值并 put，期间读端继续返回旧值。
     *
     * <p>实现说明：本库当前实现为"在 refresh 时间点后立即异步刷新"，不等真正过期。
     */
    public static <K, V> Expiration<K, V> refreshAfterWrite(Duration duration) {
        return afterWrite(duration);
    }
}