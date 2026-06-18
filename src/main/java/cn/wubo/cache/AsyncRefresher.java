package cn.wubo.cache;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 异步刷新协调器。负责在后台异步执行 refresh loader，加载完成后写入新值。
 *
 * <p>线程模型：使用调用方传入的 {@link Executor}（默认 {@code ForkJoinPool.commonPool()}）。
 * 异常处理：loader 抛出的异常被吞掉，仅记录到 {@code refreshFailureCount}，不影响缓存正确性。
 */
public final class AsyncRefresher<K, V> {

    private final Executor executor;
    private final AtomicLong refreshSuccessCount = new AtomicLong(0);
    private final AtomicLong refreshFailureCount = new AtomicLong(0);

    public AsyncRefresher(Executor executor) {
        this.executor = executor == null ? java.util.concurrent.ForkJoinPool.commonPool() : executor;
    }

    /**
     * 异步执行 refresh。
     *
     * @param cache  当前缓存实例
     * @param key    要刷新的 key
     * @param loader 加载器
     */
    public void refresh(CHMCache<K, V> cache, K key, RefreshLoader<K, V> loader) {
        try {
            executor.execute(() -> {
                try {
                    V newValue = loader.load(key);
                    if (newValue != null) {
                        cache.put(key, newValue);
                    }
                    refreshSuccessCount.incrementAndGet();
                } catch (Throwable t) {
                    refreshFailureCount.incrementAndGet();
                    // 吞掉异常，不影响缓存正确性
                }
            });
        } catch (RejectedExecutionException ree) {
            refreshFailureCount.incrementAndGet();
        }
    }

    public long refreshSuccessCount() {
        return refreshSuccessCount.get();
    }

    public long refreshFailureCount() {
        return refreshFailureCount.get();
    }
}