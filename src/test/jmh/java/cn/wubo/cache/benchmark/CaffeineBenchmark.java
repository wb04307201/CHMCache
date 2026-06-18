package cn.wubo.cache.benchmark;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
@State(Scope.Benchmark)
@Threads(8)
public class CaffeineBenchmark {

    @Param({"10000", "100000"})
    private int capacity;

    private Cache<Integer, Integer> cache;

    @Setup(Level.Trial)
    public void setup() {
        cache = Caffeine.newBuilder()
                .maximumSize(capacity)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
        for (int i = 0; i < capacity; i++) {
            cache.put(i, i);
        }
    }

    @Benchmark
    public Integer get() {
        return cache.getIfPresent(CHMCacheBenchmarkKeys.next());
    }

    @Benchmark
    public void put() {
        cache.put(CHMCacheBenchmarkKeys.next(), 0);
    }

    @Benchmark
    public Integer mixedGetPut() {
        int k = CHMCacheBenchmarkKeys.next();
        Integer v = cache.getIfPresent(k);
        if (v == null) cache.put(k, k);
        return v;
    }
}