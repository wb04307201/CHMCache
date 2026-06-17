package cn.wubo.cache.benchmark;

import cn.wubo.cache.CHMCache;
import org.openjdk.jmh.annotations.*;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
@State(Scope.Benchmark)
@Threads(8)
public class CHMCacheBenchmark {

    @Param({"10000", "100000"})
    private int capacity;

    private CHMCache<Integer, Integer> cache;

    @Setup(Level.Trial)
    public void setup() {
        cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(capacity)
                .expireAfterWrite(Duration.ofMinutes(5))
                .build();
        for (int i = 0; i < capacity; i++) {
            cache.put(i, i);
        }
    }

    @TearDown(Level.Trial)
    public void teardown() {
        cache.shutdown();
    }

    @Benchmark
    public Integer get() {
        return cache.get(CHMCacheBenchmarkKeys.next());
    }

    @Benchmark
    public void put() {
        cache.put(CHMCacheBenchmarkKeys.next(), 0);
    }

    @Benchmark
    public Integer mixedGetPut() {
        int k = CHMCacheBenchmarkKeys.next();
        Integer v = cache.get(k);
        if (v == null) {
            cache.put(k, k);
        }
        return v;
    }
}