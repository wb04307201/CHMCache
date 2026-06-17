package cn.wubo.cache;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;

import cn.wubo.cache.internal.HotKeySampler;
import cn.wubo.cache.internal.LatencyHistogram;

/**
 * 基于 {@link ConcurrentHashMap} 与 LRU 策略的高性能缓存实现。Caffeine 风格 API。
 *
 * <h2>核心设计</h2>
 * <ul>
 *   <li>三个数据结构协同：{@code ConcurrentHashMap}（无锁读写）+ 分片
 *       {@link AccessOrderTracker}（按 key hash 分桶，每桶独立 lock）+ {@link DelayQueue}（显式到期项）</li>
 *   <li>每条缓存项携带唯一 token，{@link DelayedItem} 到期回收时通过
 *       {@link ConcurrentHashMap#computeIfPresent} 做原子校验，避免误删新值</li>
 *   <li>{@code put} 路径同步触发 LRU 淘汰，避免 size 超出 {@code maximumSize}</li>
 *   <li>TTL 基于 {@link System#nanoTime()} 单调时钟</li>
 *   <li>后台清理线程（{@code daemon}）包 {@code try-catch}，异常不会静默取消后续调度</li>
 *   <li>{@link #shutdown()} 幂等</li>
 * </ul>
 *
 * @param <K> 键类型（非 null）
 * @param <V> 值类型
 */
public final class CHMCache<K, V> {

    private final String name;
    private final ConcurrentHashMap<K, CacheValue<V>> cacheMap;
    private final AccessOrderTracker<K> accessOrder;
    private final DelayQueue<DelayedItem<K>> expirationQueue;
    private final AtomicLong tokenGenerator = new AtomicLong(0L);

    private final Expiration<? super K, ? super V> expiration;
    private final Expiry<? super K, ? super V> customExpiry;
    private final Eviction eviction;
    private final Weigher<? super K, ? super V> weigher;
    private final RemovalListener<? super K, ? super V> removalListener;
    private final long maxSizeOrWeight;
    private final boolean isWeightBased;
    private final boolean recordStats;
    private final boolean slidingTtl;

    // 计数器
    private final LongAdder hitCount = new LongAdder();
    private final LongAdder missCount = new LongAdder();
    private final LongAdder evictionCount = new LongAdder();
    private final LongAdder expirationCount = new LongAdder();
    private final LongAdder loadCount = new LongAdder();
    private final LongAdder loadFailureCount = new LongAdder();
    private final AtomicLong totalLoadTimeNanos = new AtomicLong(0);
    private final AtomicLong cleanupTimeNanos = new AtomicLong(0);
    private final AtomicLong cleanupRunCount = new AtomicLong(0);
    private final AtomicLong currentWeight = new AtomicLong(0);
    private final AtomicLong sizeWatermark = new AtomicLong(0);

    // recordStats 才启用
    private final LatencyHistogram getLatency;
    private final HotKeySampler<K> hotKeySampler;

    // 后台
    private final ScheduledExecutorService cleanupExecutor;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AsyncRefresher<K, V> asyncRefresher;

    CHMCache(CHMCacheBuilder<K, V> b) {
        this.name = b.name();
        this.expiration = b.resolveExpiration();
        this.customExpiry = b.resolveExpiry();
        this.weigher = b.weigher();
        this.removalListener = b.removalListener();
        this.recordStats = b.isRecordStats();
        this.isWeightBased = b.maximumWeight() > 0;
        this.maxSizeOrWeight = isWeightBased ? b.maximumWeight() : b.maximumSize();
        this.eviction = b.resolveEviction();
        this.slidingTtl = b.slidingTtl() || customExpiry != null;
        int initialCap = Math.max(16, (int) Math.ceil(maxSizeOrWeight / 0.75f) + 1);
        this.cacheMap = new ConcurrentHashMap<>(initialCap);
        this.accessOrder = new AccessOrderTracker<>(b.shardedLocks() ? 16 : 1);
        this.expirationQueue = new DelayQueue<>();
        this.getLatency = recordStats ? new LatencyHistogram() : null;
        this.hotKeySampler = recordStats ? new HotKeySampler<>() : null;
        this.asyncRefresher = new AsyncRefresher<>(b.executor());
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chmcache-cleanup-" + name);
            t.setDaemon(true);
            return t;
        });
        startCleanupThread(b.cleanupInterval());
    }

    private void startCleanupThread(Duration interval) {
        long periodMs = Math.max(1L, interval.toMillis());
        cleanupExecutor.scheduleAtFixedRate(this::backgroundCleanupSafely,
                periodMs, periodMs, TimeUnit.MILLISECONDS);
    }

    // ============================================================
    // 静态工厂
    // ============================================================

    public static <K, V> CHMCacheBuilder<K, V> newBuilder() {
        return new CHMCacheBuilder<>();
    }

    // ============================================================
    // 基础 CRUD
    // ============================================================

    public void put(K key, V value) {
        put(key, value, Duration.ofNanos(computeTtl(key, value, true, false)));
    }

    public void put(K key, V value, Duration ttl) {
        validateKey(key);
        Objects.requireNonNull(ttl, "ttl");
        long ttlNanos = ttl.toNanos();
        if (ttlNanos <= 0) throw new IllegalArgumentException("ttl must be positive");
        long token = tokenGenerator.incrementAndGet();
        long expireNanos = System.nanoTime() + ttlNanos;
        int weight = Math.max(1, weigher.weigh(key, value));
        CacheValue<V> cv = new CacheValue<>(value, ttlNanos, token, weight);

        CacheValue<V> prior = cacheMap.put(key, cv);
        if (prior != null) {
            // 替换：触发 REPLACED 事件，扣减旧 weight
            currentWeight.addAndGet(-prior.weight);
            notifyRemoval(key, prior.value, RemovalCause.REPLACED);
        }
        currentWeight.addAndGet(weight);
        updateSizeWatermark();

        accessOrder.touch(key);

        expirationQueue.put(new DelayedItem<>(key, expireNanos, token));

        // 同步 LRU
        evictIfNeeded();
    }

    public V get(K key) {
        return get(key, (CacheLoader<K, V>) null);
    }

    public V get(K key, CacheLoader<K, V> loader) {
        validateKey(key);
        long startNs = recordStats ? System.nanoTime() : 0L;
        try {
            CacheValue<V> cv = cacheMap.get(key);
            if (cv != null && cv.isExpired()) {
                // 惰性过期
                lazyExpireIfStillExpired(key, cv);
                missCount.increment();
                recordHotKey(key);
                return loadFromLoader(key, loader);
            }
            if (cv != null) {
                hitCount.increment();
                accessOrder.touch(key);
                // 滑动 TTL：若启用，原子地替换 CacheValue 以刷新过期时间
                if (slidingTtl) {
                    refreshSlidingTtl(key, cv);
                }
                recordHotKey(key);
                return cv.value;
            }
            missCount.increment();
            recordHotKey(key);
            return loadFromLoader(key, loader);
        } finally {
            if (recordStats) {
                getLatency.record(System.nanoTime() - startNs);
            }
        }
    }

    public boolean containsKey(K key) {
        validateKey(key);
        CacheValue<V> cv = cacheMap.get(key);
        if (cv == null) return false;
        if (cv.isExpired()) {
            lazyExpireIfStillExpired(key, cv);
            return false;
        }
        return true;
    }

    public V invalidate(K key) {
        validateKey(key);
        CacheValue<V> cv = cacheMap.remove(key);
        accessOrder.remove(key);
        if (cv != null) {
            currentWeight.addAndGet(-cv.weight);
            notifyRemoval(key, cv.value, RemovalCause.EXPLICIT);
            return cv.value;
        }
        return null;
    }

    /**
     * 按 key 谓词批量失效。
     */
    public void invalidateIf(Predicate<? super K> predicate) {
        Objects.requireNonNull(predicate, "predicate");
        for (K key : cacheMap.keySet()) {
            if (predicate.test(key)) {
                invalidate(key);
            }
        }
    }

    /**
     * 清空全部。
     */
    public void invalidateAll() {
        for (Iterator<Map.Entry<K, CacheValue<V>>> it = cacheMap.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<K, CacheValue<V>> e = it.next();
            it.remove();
            currentWeight.addAndGet(-e.getValue().weight);
            notifyRemoval(e.getKey(), e.getValue().value, RemovalCause.EXPLICIT);
        }
        expirationQueue.clear();
        accessOrder.clear();
    }

    public int size() {
        return cacheMap.size();
    }

    /**
     * 当前累计权重（仅 weight-based 模式有意义）。
     */
    public long weightedSize() {
        return currentWeight.get();
    }

    /**
     * 同步 LRU 淘汰。该方法会调用 RemovalListener。
     */
    public void cleanup() {
        performCleanup(true);
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            cleanupExecutor.shutdown();
            try {
                if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    cleanupExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                cleanupExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    // ============================================================
    // 加载器
    // ============================================================

    public V computeIfAbsent(K key, Function<? super K, ? extends V> loader) {
        Objects.requireNonNull(loader, "loader");
        return get(key, k -> loader.apply(k));
    }

    public void refresh(K key, RefreshLoader<K, V> loader) {
        validateKey(key);
        Objects.requireNonNull(loader, "loader");
        asyncRefresher.refresh(this, key, loader);
    }

    private V loadFromLoader(K key, CacheLoader<K, V> loader) {
        if (loader == null) return null;
        long start = System.nanoTime();
        try {
            V loaded = loader.load(key);
            loadCount.increment();
            if (loaded != null) {
                put(key, loaded);
            }
            return loaded;
        } catch (Exception e) {
            loadFailureCount.increment();
            throw new RuntimeException("CacheLoader threw for key=" + key, e);
        } finally {
            totalLoadTimeNanos.addAndGet(System.nanoTime() - start);
        }
    }

    // ============================================================
    // 观测
    // ============================================================

    /**
     * 轻量指标快照。始终可用。
     */
    public CacheMetrics metrics() {
        return new CacheMetrics(
                cacheMap.size(),
                hitCount.sum(),
                missCount.sum(),
                evictionCount.sum(),
                expirationCount.sum(),
                cleanupTimeNanos.get(),
                cleanupRunCount.get()
        );
    }

    /**
     * 详细指标快照。仅当 {@link CHMCacheBuilder#recordStats()} 时返回有效数据。
     */
    public CacheStats stats() {
        long avgNs = getLatency != null ? (long) getLatency.averageNanos() : 0L;
        long minNs = 0, maxNs = 0;
        if (getLatency != null && getLatency.totalCount() > 0) {
            minNs = avgNs;  // 简化：不维护精确 min/max
            maxNs = avgNs;
        }
        return new CacheStats(
                hitCount.sum(),
                missCount.sum(),
                loadCount.sum(),
                loadFailureCount.sum(),
                evictionCount.sum(),
                expirationCount.sum(),
                totalLoadTimeNanos.get(),
                avgNs,
                minNs,
                maxNs,
                sizeWatermark.get()
        );
    }

    public String getName() { return name; }

    // ============================================================
    // 内部：惰性过期、淘汰、清理
    // ============================================================

    private void refreshSlidingTtl(K key, CacheValue<V> cv) {
        // 原子地替换 CacheValue：accessTimeNanos 重新置为 now，新 token 配对新的 DelayedItem。
        long now = System.nanoTime();
        long newTtlNanos = computeTtl(key, cv.value, false, true);
        long newToken = tokenGenerator.incrementAndGet();
        CacheValue<V> refreshed = new CacheValue<>(cv.value, newTtlNanos, newToken, cv.weight);
        // 因为 refreshed.createTimeNanos = now 但 accessTimeNanos = now（构造时同时赋值），
        // 实际等效于 createTime 刷新。等价实现：保留旧 weight，重新 put 一个 accessTimeNanos=now 的实例
        if (cacheMap.replace(key, cv, refreshed)) {
            expirationQueue.put(new DelayedItem<>(key, now + newTtlNanos, newToken));
        }
        // 替换失败说明并发修改，不做处理（下次 get 重新触发）
    }

    private void lazyExpireIfStillExpired(K key, CacheValue<V> cv) {
        boolean[] removed = {false};
        cacheMap.computeIfPresent(key, (k, v) -> {
            if (v.token == cv.token && v.isExpired()) {
                removed[0] = true;
                return null;
            }
            return v;
        });
        if (removed[0]) {
            expirationCount.increment();
            accessOrder.remove(key);
            currentWeight.addAndGet(-cv.weight);
            notifyRemoval(key, cv.value, RemovalCause.EXPIRED);
        }
    }

    private void evictIfNeeded() {
        if (eviction.shouldEvict(cacheMap.size(), isWeightBased ? -1 : maxSizeOrWeight,
                currentWeight.get(), isWeightBased ? maxSizeOrWeight : -1)) {
            doEvict();
        }
    }

    private void doEvict() {
        // 分段 LRU 遍历
        int targetEvictions = computeEvictionCount();
        int evicted = 0;
        for (int stripe = 0; stripe < accessOrder.stripeCount() && evicted < targetEvictions; stripe++) {
            LinkedHashMap<K, Boolean> seg = accessOrder.segmentAt(stripe);
            synchronized (accessOrder.stripedLocks().lockFor("seg-" + stripe)) {
                Iterator<Map.Entry<K, Boolean>> it = seg.entrySet().iterator();
                while (it.hasNext() && evicted < targetEvictions) {
                    Map.Entry<K, Boolean> e = it.next();
                    K key = e.getKey();
                    CacheValue<V> removed = cacheMap.remove(key);
                    it.remove();
                    if (removed != null) {
                        currentWeight.addAndGet(-removed.weight);
                        notifyRemoval(key, removed.value,
                                isWeightBased ? RemovalCause.WEIGHT : RemovalCause.SIZE);
                        evictionCount.increment();
                        evicted++;
                    }
                }
            }
        }
    }

    private int computeEvictionCount() {
        if (isWeightBased) {
            long excess = currentWeight.get() - maxSizeOrWeight;
            return excess <= 0 ? 0 : (int) Math.min(excess, 1024);
        } else {
            int excess = cacheMap.size() - (int) maxSizeOrWeight;
            return Math.max(0, Math.min(excess, 1024));
        }
    }

    private void backgroundCleanupSafely() {
        try {
            performCleanup(false);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void performCleanup(boolean fullScan) {
        long start = System.nanoTime();
        try {
            // 排空延迟队列
            List<K> toRemove = new ArrayList<>();
            while (true) {
                DelayedItem<K> item = expirationQueue.poll();
                if (item == null) break;
                boolean[] removed = {false};
                cacheMap.computeIfPresent(item.key, (k, v) -> {
                    if (v.token == item.token) {
                        removed[0] = true;
                        return null;
                    }
                    return v;
                });
                if (removed[0]) {
                    CacheValue<V> cv = cacheMap.get(item.key); // 已 null
                    toRemove.add(item.key);
                    expirationCount.increment();
                }
            }
            // 全量/采样扫描
            if (fullScan) {
                for (K key : cacheMap.keySet()) {
                    tryExpireKey(key);
                }
            } else {
                int size = cacheMap.size();
                if (size > 0) {
                    int sampleSize = Math.min(100, Math.max(1, size / 10));
                    List<K> samples = new ArrayList<>(cacheMap.keySet());
                    Collections.shuffle(samples, ThreadLocalRandom.current());
                    int n = Math.min(sampleSize, samples.size());
                    for (int i = 0; i < n; i++) {
                        tryExpireKey(samples.get(i));
                    }
                }
            }
            // LRU 兜底
            evictIfNeeded();
        } finally {
            cleanupTimeNanos.addAndGet(System.nanoTime() - start);
            cleanupRunCount.incrementAndGet();
        }
    }

    private void tryExpireKey(K key) {
        CacheValue<V> cv = cacheMap.get(key);
        if (cv != null && cv.isExpired()) {
            lazyExpireIfStillExpired(key, cv);
        }
    }

    private void notifyRemoval(K key, V value, RemovalCause cause) {
        if (removalListener != null) {
            try {
                removalListener.onRemoval(key, value, cause);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private void recordHotKey(K key) {
        if (hotKeySampler != null) hotKeySampler.record(key);
    }

    private void updateSizeWatermark() {
        long s = cacheMap.size();
        long prev;
        do {
            prev = sizeWatermark.get();
            if (s <= prev) return;
        } while (!sizeWatermark.compareAndSet(prev, s));
    }

    private long computeTtl(K key, V value, boolean isCreate, boolean isRead) {
        long now = System.nanoTime();
        if (customExpiry != null) {
            Duration d;
            if (isRead) d = customExpiry.expireAfterRead(key, value, now);
            else if (isCreate) d = customExpiry.expireAfterCreate(key, value, now);
            else d = customExpiry.expireAfterUpdate(key, value, now);
            return Math.max(0L, d.toNanos());
        }
        Duration d;
        if (isRead) d = expiration.expireAfterRead(key, value, now);
        else if (isCreate) d = expiration.expireAfterCreate(key, value, now);
        else d = expiration.expireAfterUpdate(key, value, now);
        return Math.max(0L, d.toNanos());
    }

    private static <K> void validateKey(K key) {
        if (key == null) throw new NullPointerException("key must not be null");
    }
}