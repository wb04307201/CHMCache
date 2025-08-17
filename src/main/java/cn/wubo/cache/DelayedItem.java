package cn.wubo.cache;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class DelayedItem<K> implements Delayed {
    final K key;
    final long expireTime;

    DelayedItem(K key, long expireTime) {
        this.key = key;
        this.expireTime = expireTime;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(expireTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        return Long.compare(this.expireTime, ((DelayedItem<?>) o).expireTime);
    }
}
