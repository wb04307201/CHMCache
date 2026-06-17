package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class ShardedLockTest {

    @Test
    void testShardedLocksConcurrent() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2000)
                .expireAfterWrite(Duration.ofSeconds(60))
                .shardedLocks(true)
                .build();
        concurrentStress(cache);
    }

    @Test
    void testNonShardedLocksConcurrent() throws Exception {
        CHMCache<Integer, Integer> cache = CHMCache.<Integer, Integer>newBuilder()
                .maximumSize(2000)
                .expireAfterWrite(Duration.ofSeconds(60))
                .shardedLocks(false)
                .build();
        concurrentStress(cache);
    }

    private void concurrentStress(CHMCache<Integer, Integer> cache) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            int threads = 8;
            int ops = 1_000;
            CyclicBarrier barrier = new CyclicBarrier(threads);
            CountDownLatch done = new CountDownLatch(threads);
            AtomicInteger errors = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int seed = t;
                futures.add(pool.submit(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < ops; i++) {
                            try {
                                int k = (seed * 17 + i) % 500;
                                if (i % 4 == 0) cache.put(k, i);
                                else if (i % 4 == 1) cache.get(k);
                                else if (i % 4 == 2) cache.containsKey(k);
                                else cache.invalidate(k);
                            } catch (Throwable th) {
                                errors.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        errors.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }));
            }
            assertTrue(done.await(20, TimeUnit.SECONDS), "concurrent ops did not finish");
            for (Future<?> f : futures) f.get(5, TimeUnit.SECONDS);
            assertEquals(0, errors.get(), "no errors expected under concurrent stress");
            assertTrue(cache.size() <= 2000, "size must respect maximumSize");
        } finally {
            pool.shutdownNow();
            cache.shutdown();
        }
    }
}