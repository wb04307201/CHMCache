package cn.wubo.cache.benchmark;

import org.openjdk.jmh.annotations.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Fork(2)
@State(Scope.Benchmark)
@Threads(8)
public class ConcurrentHashMapBenchmark {

    @Param({"10000", "100000"})
    private int capacity;

    private ConcurrentHashMap<Integer, Integer> map;

    @Setup(Level.Trial)
    public void setup() {
        map = new ConcurrentHashMap<>(capacity);
        for (int i = 0; i < capacity; i++) {
            map.put(i, i);
        }
    }

    @Benchmark
    public Integer get() {
        return map.get(CHMCacheBenchmarkKeys.next());
    }

    @Benchmark
    public void put() {
        map.put(CHMCacheBenchmarkKeys.next(), 0);
    }

    @Benchmark
    public Integer mixedGetPut() {
        int k = CHMCacheBenchmarkKeys.next();
        Integer v = map.get(k);
        if (v == null) map.put(k, k);
        return v;
    }
}