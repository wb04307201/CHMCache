package cn.wubo.cache.internal;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 按 key hash 分桶的分片锁。共 N=16 段，相同 hash 桶内的 key 共享同一把锁。
 *
 * <p>用于解决单 {@link ReentrantLock} 保护全量访问顺序时的竞争瓶颈（Tier 3.1）。
 *
 * <p>非线程安全构造（仅初始化时使用一次）。
 */
public final class StripedLocks {

    /** 默认分片数。 */
    public static final int DEFAULT_STRIPES = 16;

    private final ReentrantLock[] locks;
    private final int mask;

    public StripedLocks() {
        this(DEFAULT_STRIPES);
    }

    public StripedLocks(int stripes) {
        if (stripes <= 0 || (stripes & (stripes - 1)) != 0) {
            throw new IllegalArgumentException("stripes must be a positive power of 2, was " + stripes);
        }
        this.locks = new ReentrantLock[stripes];
        for (int i = 0; i < stripes; i++) {
            locks[i] = new ReentrantLock();
        }
        this.mask = stripes - 1;
    }

    public int stripeCount() {
        return locks.length;
    }

    /**
     * 计算给定 key 所属的段索引。
     */
    public int stripeFor(Object key) {
        int h = key == null ? 0 : key.hashCode();
        // spread bits to avoid poor distribution on identity hashCode
        h ^= (h >>> 16);
        return h & mask;
    }

    public ReentrantLock lockFor(Object key) {
        return locks[stripeFor(key)];
    }

    /**
     * 在指定 key 的段锁内执行动作。
     */
    public void withLock(Object key, Runnable action) {
        ReentrantLock lock = lockFor(key);
        lock.lock();
        try {
            action.run();
        } finally {
            lock.unlock();
        }
    }
}