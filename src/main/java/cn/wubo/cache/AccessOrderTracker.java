package cn.wubo.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

import cn.wubo.cache.internal.StripedLocks;

/**
 * 访问顺序（LRU）跟踪器。基于分片 LinkedHashMap，每段独立 lock。
 *
 * <p><b>设计权衡</b>：段内 LRU 是严格的，段间是近似的。当某段超出其应占份额时，
 * 只淘汰该段内最旧的条目，全局并非严格 LRU —— 这是性能换取的取舍。
 *
 * <p>段数默认为 16（与 Caffeine 一致），可通过构造参数调整。
 */
public final class AccessOrderTracker<K> {

    private final LinkedHashMap<K, Boolean>[] segments;
    private final StripedLocks stripedLocks;

    @SuppressWarnings("unchecked")
    public AccessOrderTracker() {
        this(StripedLocks.DEFAULT_STRIPES);
    }

    @SuppressWarnings("unchecked")
    public AccessOrderTracker(int stripeCount) {
        this.stripedLocks = new StripedLocks(stripeCount);
        this.segments = new LinkedHashMap[stripeCount];
        for (int i = 0; i < stripeCount; i++) {
            segments[i] = new LinkedHashMap<>(16, 0.75f, true);
        }
    }

    public int stripeCount() {
        return segments.length;
    }

    public StripedLocks stripedLocks() {
        return stripedLocks;
    }

    /**
     * 标记 key 被访问（get 或 put 命中）。在 key 所属段的锁内执行。
     */
    public void touch(K key) {
        stripedLocks.withLock(key, () -> segmentFor(key).put(key, Boolean.TRUE));
    }

    /**
     * 标记 key 被访问，但用调用方已获取的锁（供 CHMCache 内部优化）。
     */
    public void touchUnderLock(K key, int stripe) {
        segments[stripe].put(key, Boolean.TRUE);
    }

    /**
     * 删除 key（用于 invalidate / 淘汰 / 过期清理）。在段锁内执行。
     */
    public void remove(K key) {
        stripedLocks.withLock(key, () -> segmentFor(key).remove(key));
    }

    public void removeUnderLock(K key, int stripe) {
        segments[stripe].remove(key);
    }

    /**
     * 获取 key 所属段。
     */
    public LinkedHashMap<K, Boolean> segmentFor(K key) {
        return segments[stripedLocks.stripeFor(key)];
    }

    public LinkedHashMap<K, Boolean> segmentAt(int stripe) {
        return segments[stripe];
    }

    /**
     * 遍历所有段，对每段的最旧条目（accessOrder=true 时 entrySet().iterator() 即从最旧到最新）
     * 依次调用 consumer，直到 consumer 返回 true 或所有段都被访问过。
     *
     * <p>供 LRU 淘汰使用。consumer 在段锁内被调用。
     *
     * @return 被淘汰的条目总数
     */
    public int drainOldest(BiConsumer<K, Boolean> perEntry) {
        int removed = 0;
        for (int stripe = 0; stripe < segments.length; stripe++) {
            LinkedHashMap<K, Boolean> seg = segments[stripe];
            // 修复 Bug 5:必须使用 ReentrantLock 与 touch/remove 一致,
            // 否则 synchronized 与 ReentrantLock 不互斥,LinkedHashMap.iterator() 抛 CME
            ReentrantLock lock = stripedLocks.lockFor(stripe);
            lock.lock();
            try {
                Iterator<Map.Entry<K, Boolean>> it = seg.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<K, Boolean> e = it.next();
                    K key = e.getKey();
                    perEntry.accept(key, e.getValue());
                    it.remove();
                    removed++;
                }
            } finally {
                lock.unlock();
            }
        }
        return removed;
    }

    /**
     * 清空所有段。
     */
    public void clear() {
        for (int stripe = 0; stripe < segments.length; stripe++) {
            // 修复 Bug 5:统一使用 ReentrantLock
            ReentrantLock lock = stripedLocks.lockFor(stripe);
            lock.lock();
            try {
                segments[stripe].clear();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 统计所有段的条目总数。
     */
    public int size() {
        int total = 0;
        for (LinkedHashMap<K, Boolean> seg : segments) {
            total += seg.size();
        }
        return total;
    }
}