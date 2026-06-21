package cn.wubo.cache;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * 延迟队列元素。基于可注入的单调时钟（默认 {@link System#nanoTime()}）。
 */
final class DelayedItem<K> implements Delayed {

    final K key;
    final long expireTimeNanos;
    final long token;
    private final LongSupplier nanoTimeSource;

    DelayedItem(K key, long expireTimeNanos, long token, LongSupplier nanoTimeSource) {
        this.key = key;
        this.expireTimeNanos = expireTimeNanos;
        this.token = token;
        this.nanoTimeSource = nanoTimeSource;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        long remaining = expireTimeNanos - nanoTimeSource.getAsLong();
        return unit.convert(remaining, TimeUnit.NANOSECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.expireTimeNanos, ((DelayedItem<?>) o).expireTimeNanos);
    }
}