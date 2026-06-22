package cn.wubo.cache.internal;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * 内部：单个 key 的统计计数器。
 * 仅在 {@code CHMCacheBuilder.enablePerKeyMetrics(true)} 时被分配。
 */
public final class PerKeyStats {
    public final LongAdder hitCount = new LongAdder();
    public final LongAdder missCount = new LongAdder();
    public final LongAdder putCount = new LongAdder();
    public final LongAdder evictionCount = new LongAdder();
    public final LongAdder expirationCount = new LongAdder();
    public final AtomicLong lastPutEpochMs = new AtomicLong(0);
    public final AtomicLong lastHitEpochMs = new AtomicLong(0);
}
