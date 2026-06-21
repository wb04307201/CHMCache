package cn.wubo.cache;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.LongSupplier;

/**
 * CHMCache 的不可变配置记录。通过 {@link CHMCacheBuilder} 构造。
 *
 * <p>集中校验所有配置字段的合法性（null / 负值 / 互斥冲突），避免校验逻辑分散在 builder
 * setter 与运行时构造器里。Builder API 保持不变，仅在内部产物改为 record，便于未来扩展
 * 字段与单元测试。</p>
 *
 * @since 1.1.0
 */
public record CHMCacheConfig<K, V>(
        String name,
        long maximumSize,
        long maximumWeight,
        Weigher<? super K, ? super V> weigher,
        Eviction eviction,
        Expiration<? super K, ? super V> expiration,
        Expiry<? super K, ? super V> expiry,
        Duration cleanupInterval,
        RemovalListener<? super K, ? super V> removalListener,
        CacheListener<? super K, ? super V> listener,
        boolean statsEnabled,
        boolean shardedLocks,
        boolean slidingTtl,
        Duration refreshAfterWriteDuration,
        Executor executor,
        LongSupplier nanoTimeSource,
        boolean perKeyMetrics
) {
    public CHMCacheConfig {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(weigher, "weigher");
        Objects.requireNonNull(eviction, "eviction");
        Objects.requireNonNull(expiration, "expiration");
        Objects.requireNonNull(cleanupInterval, "cleanupInterval");
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(nanoTimeSource, "nanoTimeSource");
        if (maximumSize <= 0 && maximumWeight <= 0) {
            throw new IllegalStateException("must configure maximumSize or maximumWeight");
        }
        if (maximumSize > 0 && maximumWeight > 0) {
            throw new IllegalStateException("cannot configure both maximumSize and maximumWeight");
        }
        if (cleanupInterval.isNegative() || cleanupInterval.isZero()) {
            throw new IllegalArgumentException("cleanupInterval must be positive");
        }
    }

    /** @return 默认配置（仅用于测试，生产环境必须通过 Builder 配置） */
    public static <K, V> CHMCacheConfig<K, V> defaults() {
        return CHMCache.<K, V>newBuilder()
                .maximumSize(1000)
                .buildConfig();
    }
}
