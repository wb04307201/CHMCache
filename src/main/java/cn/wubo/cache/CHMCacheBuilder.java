package cn.wubo.cache;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;

/**
 * {@link CHMCache} 的构建器。Caffeine 风格命名。
 *
 * <p>典型用法：
 * <pre>{@code
 * CHMCache<String, User> cache = CHMCache.<String, User>newBuilder()
 *     .maximumSize(10_000)
 *     .expireAfterWrite(Duration.ofMinutes(5))
 *     .removalListener((k, v, cause) -> log.info("{} removed ({})", k, cause))
 *     .recordStats()
 *     .build();
 * }</pre>
 *
 * @param <K> 键类型
 * @param <V> 值类型
 */
public final class CHMCacheBuilder<K, V> {

    // 默认值
    static final long DEFAULT_CLEANUP_INTERVAL_NANOS = Duration.ofSeconds(1).toNanos();

    private String name = "default";
    private long maximumSize = -1;
    private long maximumWeight = -1;
    private Weigher<? super K, ? super V> weigher = Weigher.singletonWeigher();
    private Eviction eviction = null; // 由 build() 根据配置决定
    private Expiration<? super K, ? super V> expiration = null; // 同上
    private Expiry<? super K, ? super V> expiry = null;
    private Duration cleanupInterval = Duration.ofSeconds(1);
    private RemovalListener<? super K, ? super V> removalListener = null;
    private boolean statsEnabled = false;
    private boolean shardedLocks = true;
    private boolean slidingTtl = false;
    private Duration refreshAfterWriteDuration = null;
    private Executor executor = ForkJoinPool.commonPool();

    CHMCacheBuilder() {}

    public String name() { return name; }
    public long maximumSize() { return maximumSize; }
    public long maximumWeight() { return maximumWeight; }
    public Weigher<? super K, ? super V> weigher() { return weigher; }
    public Expiration<? super K, ? super V> expiration() { return expiration; }
    public Expiry<? super K, ? super V> expiry() { return expiry; }
    public Duration cleanupInterval() { return cleanupInterval; }
    public boolean isRecordStats() { return statsEnabled; }
    public boolean shardedLocks() { return shardedLocks; }
    public boolean slidingTtl() { return slidingTtl; }
    public Executor executor() { return executor; }
    public RemovalListener<? super K, ? super V> removalListener() { return removalListener; }
    public Duration refreshAfterWriteDuration() { return refreshAfterWriteDuration; }

    /**
     * 设置缓存实例名称。默认 "default"。
     */
    public CHMCacheBuilder<K, V> name(String name) {
        this.name = Objects.requireNonNull(name, "name");
        return this;
    }

    /**
     * 设置最大条目数。
     */
    public CHMCacheBuilder<K, V> maximumSize(long maximumSize) {
        if (maximumSize <= 0) throw new IllegalArgumentException("maximumSize must be > 0");
        if (this.maximumWeight > 0) {
            throw new IllegalStateException("cannot configure both maximumSize and maximumWeight");
        }
        this.maximumSize = maximumSize;
        return this;
    }

    /**
     * 设置最大累计权重。必须同时配置 {@link #weigher(Weigher)}。
     */
    public <K1 extends K, V1 extends V> CHMCacheBuilder<K1, V1> maximumWeight(long maximumWeight) {
        if (maximumWeight <= 0) throw new IllegalArgumentException("maximumWeight must be > 0");
        if (this.maximumSize > 0) {
            throw new IllegalStateException("cannot configure both maximumSize and maximumWeight");
        }
        CHMCacheBuilder<K1, V1> casted = (CHMCacheBuilder<K1, V1>) this;
        casted.maximumWeight = maximumWeight;
        return casted;
    }

    /**
     * 设置条目权重计算器。
     */
    public CHMCacheBuilder<K, V> weigher(Weigher<? super K, ? super V> weigher) {
        this.weigher = Objects.requireNonNull(weigher, "weigher");
        return this;
    }

    /**
     * 写后过期（默认行为）。
     */
    public CHMCacheBuilder<K, V> expireAfterWrite(Duration duration) {
        this.expiration = Expirations.afterWrite(duration);
        // 显式写后过期 = 固定 TTL,关闭 sliding 以遵循用户最后一次显式声明
        this.slidingTtl = false;
        return this;
    }

    /**
     * 滑动 TTL：get 命中刷新过期时间。
     */
    public CHMCacheBuilder<K, V> expireAfterAccess(Duration duration) {
        this.expiration = Expirations.afterAccess(duration);
        this.slidingTtl = true;
        return this;
    }

    /**
     * 写后异步刷新：经过指定时长后，后台线程会异步触发 {@link RefreshLoader} 重新加载值。
     * 加载期间读端仍返回旧值，加载完成后替换。值的过期时间为 refresh 窗口的 2 倍，
     * 给后台刷新留出充足时间；如果希望更短的过期时间，可显式叠加 {@link #expireAfterWrite}。
     */
    public CHMCacheBuilder<K, V> refreshAfterWrite(Duration duration) {
        Objects.requireNonNull(duration, "duration");
        this.refreshAfterWriteDuration = duration;
        if (this.expiration == null) {
            // TTL = 2 × refresh window，留出时间让后台异步加载完成
            this.expiration = Expirations.afterWrite(duration.multipliedBy(2));
        }
        return this;
    }

    /**
     * 自定义过期策略（覆盖 expireAfterWrite/Access 等设置）。
     */
    public CHMCacheBuilder<K, V> expireAfter(Expiry<? super K, ? super V> expiry) {
        this.expiry = Objects.requireNonNull(expiry, "expiry");
        return this;
    }

    /**
     * 设置后台清理线程的调度周期（同时影响延迟队列清理与过期回收）。
     */
    public CHMCacheBuilder<K, V> cleanupInterval(Duration interval) {
        if (interval == null || interval.isNegative() || interval.isZero())
            throw new IllegalArgumentException("cleanupInterval must be positive");
        this.cleanupInterval = interval;
        return this;
    }

    /**
     * 设置条目移除监听器。
     */
    public CHMCacheBuilder<K, V> removalListener(RemovalListener<? super K, ? super V> listener) {
        this.removalListener = Objects.requireNonNull(listener, "removalListener");
        return this;
    }

    /**
     * 启用详细统计（延迟分布、热点 Key 等开销较高的指标）。
     */
    public CHMCacheBuilder<K, V> recordStats() {
        this.statsEnabled = true;
        return this;
    }

    /**
     * 设置是否启用分片锁。默认 true（高并发场景）。单线程或测试时可关闭以减少开销。
     */
    public CHMCacheBuilder<K, V> shardedLocks(boolean sharded) {
        this.shardedLocks = sharded;
        return this;
    }

    /**
     * 自定义异步任务执行器（用于 refresh）。
     */
    public CHMCacheBuilder<K, V> executor(Executor executor) {
        this.executor = Objects.requireNonNull(executor, "executor");
        return this;
    }

    /**
     * 构建缓存实例。
     */
    public CHMCache<K, V> build() {
        validate();
        return new CHMCache<>(this);
    }

    private void validate() {
        if (maximumSize < 0 && maximumWeight < 0) {
            throw new IllegalStateException("must configure maximumSize or maximumWeight");
        }
        if (maximumSize > 0 && maximumWeight > 0) {
            throw new IllegalStateException("cannot configure both maximumSize and maximumWeight");
        }
        if (maximumWeight > 0) {
            // weigher 由用户显式设置（即使是默认 1），不强制使用 singletonWeigher
            this.eviction = Evictions.weightBased();
        } else {
            this.eviction = Evictions.sizeBased();
        }
        if (expiration == null) {
            // 默认 expireAfterWrite(forever) —— 不过期
            this.expiration = Expirations.afterWrite(Duration.ofNanos(Long.MAX_VALUE / 2));
        }
    }

    /**
     * 暴露给 CHMCache 内部使用：获取对应的 Eviction 实例。
     */
    Eviction resolveEviction() {
        return eviction;
    }

    /**
     * 暴露给 CHMCache 内部使用：获取对应的 Expiration 实例。
     */
    Expiration<? super K, ? super V> resolveExpiration() {
        return expiration;
    }

    /**
     * 暴露给 CHMCache 内部使用：获取自定义 Expiry（可为 null）。
     */
    Expiry<? super K, ? super V> resolveExpiry() {
        return expiry;
    }
}