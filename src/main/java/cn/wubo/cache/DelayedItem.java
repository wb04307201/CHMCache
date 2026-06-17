package cn.wubo.cache;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/**
 * 延迟队列元素。基于 {@link System#nanoTime()} 单调时钟。
 */
final class DelayedItem<K> implements Delayed {

    final K key;
    final long expireTimeNanos;
    final long token;

    DelayedItem(K key, long expireTimeNanos, long token) {
        this.key = key;
        this.expireTimeNanos = expireTimeNanos;
        this.token = token;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long remaining = expireTimeNanos - System.nanoTime();
        return unit.convert(remaining, TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.expireTimeNanos, ((DelayedItem<?>) o).expireTimeNanos);
    }
}