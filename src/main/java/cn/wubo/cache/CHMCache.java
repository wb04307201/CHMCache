
package cn.wubo.cache;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个基于 ConcurrentHashMap 和 LRU 策略的高性能缓存实现。
 * 支持自动过期、大小限制、LRU 淘汰、后台清理等特性。
 */
public class CHMCache<K, V> {

    // 主缓存存储：使用 ConcurrentHashMap 存储键值对和过期时间
    private final ConcurrentHashMap<K, CacheValue<V>> cacheMap;

    // 访问顺序链表：用于维护访问顺序，实现 LRU 淘汰策略
    private final LinkedHashMap<K, Long> accessOrderMap;
    private final ReentrantLock lruLock = new ReentrantLock(); // LRU 操作的锁

    // 延迟队列：用于处理明确过期项（基于 DelayQueue 实现）
    private final DelayQueue<DelayedItem<K>> expirationQueue;

    // 缓存配置参数
    private final int maxSize; // 最大缓存大小
    private final long defaultTtlMillis; // 默认 TTL（毫秒）
    private final Random ttlRandom; // 用于随机化 TTL 的随机数生成器

    // 监控指标统计
    private final AtomicLong hitCount = new AtomicLong(0); // 命中次数
    private final AtomicLong missCount = new AtomicLong(0); // 未命中次数
    private final AtomicLong evictionCount = new AtomicLong(0); // 淘汰次数
    private final AtomicLong cleanupTimeNanos = new AtomicLong(0); // 清理耗时（纳秒）

    // 清理线程池：用于定期执行清理任务
    private final ScheduledExecutorService cleanupExecutor;

    /**
     * 默认构造函数，使用默认的缓存大小1000和默认的 TTL 60秒
     */
    public CHMCache() {
        this(1000, 60_000, TimeUnit.MILLISECONDS);
    }

    /**
     * 构造函数，初始化缓存配置和数据结构
     * @param maxSize 缓存最大大小
     * @param defaultTtlMillis 默认过期时间（毫秒）
     */
    public CHMCache(int maxSize, long defaultTtlMillis) {
        this(maxSize, defaultTtlMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 构造函数，初始化缓存配置和数据结构
     * @param maxSize 缓存最大大小
     * @param defaultTtlMillis 默认过期时间（毫秒）
     * @param unit 时间单位
     */
    public CHMCache(int maxSize, long defaultTtlMillis, TimeUnit unit) {
        this.maxSize = maxSize;
        this.defaultTtlMillis = unit.toMillis(defaultTtlMillis);
        this.ttlRandom = new Random();
        this.cacheMap = new ConcurrentHashMap<>();
        this.accessOrderMap = new LinkedHashMap<>(16, 0.75f, true); // accessOrder=true 表示按访问顺序排序
        this.expirationQueue = new DelayQueue<>();
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor();

        // 启动后台清理线程
        startCleanupThread();
    }

    /**
     * 启动后台定时清理线程，定期执行清理任务
     */
    private void startCleanupThread() {
        cleanupExecutor.scheduleAtFixedRate(() -> {
            long start = System.nanoTime();
            int cleaned = 0;

            // 处理延迟队列中的过期项
            while (true) {
                DelayedItem<K> delayedItem = expirationQueue.poll();
                if (delayedItem == null) {
                    break;
                }
                cleaned++;
                cacheMap.remove(delayedItem.key);
                accessOrderMap.remove(delayedItem.key);
            }

            // 随机采样清理过期项（惰性删除的一部分）
            int sampleSize = Math.min(100, cacheMap.size() / 10);
            if (sampleSize > 0) {
                List<K> samples = new ArrayList<>(cacheMap.keySet());
                Collections.shuffle(samples);
                for (int i = 0; i < Math.min(sampleSize, samples.size()); i++) {
                    K key = samples.get(i);
                    CacheValue<V> cacheValue = cacheMap.get(key);
                    if (cacheValue != null && cacheValue.isExpired()) {
                        cacheMap.remove(key);
                        accessOrderMap.remove(key);
                        cleaned++;
                    }
                }
            }

            // LRU清理（如果超过大小限制）
            if (cacheMap.size() > maxSize) {
                cleaned += enforceLRU();
            }

            if (cleaned > 0) {
                cleanupTimeNanos.addAndGet(System.nanoTime() - start);
            }
        }, 1, 1, TimeUnit.SECONDS); // 每秒执行一次清理
    }

    /**
     * 强制执行 LRU 淘汰策略，移除最近最少使用的缓存项
     * @return 被移除的缓存项数量
     */
    private int enforceLRU() {
        lruLock.lock();
        try {
            int removed = 0;
            Iterator<Map.Entry<K, Long>> iterator = accessOrderMap.entrySet().iterator();
            while (cacheMap.size() > maxSize && iterator.hasNext()) {
                Map.Entry<K, Long> entry = iterator.next();
                K key = entry.getKey();
                if (cacheMap.remove(key) != null) {
                    iterator.remove();
                    removed++;
                    evictionCount.incrementAndGet();
                }
            }
            return removed;
        } finally {
            lruLock.unlock();
        }
    }

    /**
     * 随机化 TTL（在默认 TTL 基础上 ±20%）
     * @return 随机化的 TTL 值
     */
    private long getRandomizedTtl() {
        double factor = 0.8 + 0.4 * ttlRandom.nextDouble(); // 0.8-1.2范围
        return (long) (defaultTtlMillis * factor);
    }

    /**
     * 添加缓存项，默认使用随机 TTL
     * @param key 键
     * @param value 值
     */
    public void put(K key, V value) {
        put(key, value, getRandomizedTtl());
    }

    /**
     * 添加缓存项，指定 TTL
     * @param key 键
     * @param value 值
     * @param ttlMillis 过期时间（毫秒）
     */
    public void put(K key, V value, long ttlMillis) {
        put(key, value, ttlMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 添加缓存项，指定 TTL
     * @param key 键
     * @param value 值
     * @param ttlMillis 过期时间（毫秒）
     * @param unit 时间单位
     */
    public void put(K key, V value, long ttlMillis, TimeUnit unit) {
        long millis = unit.toMillis(ttlMillis);
        long expireTime = System.currentTimeMillis() + millis;
        CacheValue<V> cacheValue = new CacheValue<>(value, millis);

        // 添加到主缓存
        cacheMap.put(key, cacheValue);

        // 更新访问顺序
        lruLock.lock();
        try {
            accessOrderMap.put(key, System.nanoTime());
        } finally {
            lruLock.unlock();
        }

        // 添加到延迟队列
        expirationQueue.put(new DelayedItem<>(key, expireTime));
    }

    /**
     * 获取缓存项
     * @param key 键
     * @return 值，如果不存在或已过期则返回 null
     */
    public V get(K key) {
        CacheValue<V> cacheValue = cacheMap.get(key);

        // 惰性删除：检查是否过期
        if (cacheValue != null && cacheValue.isExpired()) {
            cacheMap.remove(key);
            accessOrderMap.remove(key);
            missCount.incrementAndGet();
            return null;
        }

        if (cacheValue != null) {
            hitCount.incrementAndGet();
            // 更新访问顺序
            lruLock.lock();
            try {
                accessOrderMap.put(key, System.nanoTime());
            } finally {
                lruLock.unlock();
            }
            return cacheValue.value;
        }

        missCount.incrementAndGet();
        return null;
    }

    /**
     * 删除缓存项
     * @param key 键
     * @return 被删除的值，如果不存在则返回 null
     */
    public V remove(K key) {
        CacheValue<V> cacheValue = cacheMap.remove(key);
        accessOrderMap.remove(key);
        if (cacheValue != null) {
            return cacheValue.value;
        }
        return null;
    }

    /**
     * 清理所有过期项
     */
    public void cleanup() {
        long start = System.nanoTime();
        int cleaned = 0;

        // 处理延迟队列
        DelayedItem<K> delayedItem;
        while ((delayedItem = expirationQueue.poll()) != null) {
            cleaned++;
            cacheMap.remove(delayedItem.key);
            accessOrderMap.remove(delayedItem.key);
        }

        // 全面扫描清理
        for (K key : new ArrayList<>(cacheMap.keySet())) {
            CacheValue<V> cacheValue = cacheMap.get(key);
            if (cacheValue != null && cacheValue.isExpired()) {
                cacheMap.remove(key);
                accessOrderMap.remove(key);
                cleaned++;
            }
        }

        // 执行LRU清理
        cleaned += enforceLRU();

        if (cleaned > 0) {
            cleanupTimeNanos.addAndGet(System.nanoTime() - start);
        }
    }

    /**
     * 获取当前缓存大小
     * @return 缓存项数量
     */
    public int size() {
        return cacheMap.size();
    }

    /**
     * 关闭缓存，停止清理线程
     */
    public void shutdown() {
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

    /**
     * 获取监控指标
     */
    public MonitorMetrics getMetrics() {
        return new MonitorMetrics(
                hitCount.get(),
                missCount.get(),
                evictionCount.get(),
                cleanupTimeNanos.get(),
                cacheMap.size()
        );
    }
}