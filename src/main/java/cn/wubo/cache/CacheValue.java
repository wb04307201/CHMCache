package cn.wubo.cache;

public class CacheValue<V> {
    final V value;
    final long createTime;
    final long ttlMillis;

    CacheValue(V value, long ttlMillis) {
        this.value = value;
        this.createTime = System.currentTimeMillis();
        this.ttlMillis = ttlMillis;
    }

    boolean isExpired() {
        return System.currentTimeMillis() - createTime > ttlMillis;
    }
}
