package cn.wubo.cache.benchmark;

/**
 * 跨 benchmark 复用的 key 生成器，避免每个测试自己维护种子逻辑。
 */
public final class CHMCacheBenchmarkKeys {

    private static final ThreadLocal<long[]> STATE = ThreadLocal.withInitial(() -> new long[]{System.nanoTime(), 0x9E3779B97F4A7C15L});

    private CHMCacheBenchmarkKeys() {}

    public static int next() {
        long[] s = STATE.get();
        long x = s[0] += 0x9E3779B97F4A7C15L;
        x = (x ^ (x >>> 30)) * 0xBF58476D1CE4E5B9L;
        x = (x ^ (x >>> 27)) * 0x94D049BB133111EBL;
        x = x ^ (x >>> 31);
        return (int) Math.floorMod(x, 10_000L);
    }
}