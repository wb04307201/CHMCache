package cn.wubo.cache;

import cn.wubo.cache.internal.StripedLocks;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class AccessOrderTrackerTest {

    @Test
    void testBasicTouchAndRemove() {
        AccessOrderTracker<String> t = new AccessOrderTracker<>(4);
        t.touch("a");
        t.touch("b");
        assertEquals(2, t.size());
        t.remove("a");
        assertEquals(1, t.size());
    }

    @Test
    void testStripeForDistribution() {
        AccessOrderTracker<Integer> t = new AccessOrderTracker<>(16);
        // 16 个段都有分布
        boolean[] seen = new boolean[16];
        for (int i = 0; i < 1000; i++) {
            int s = t.stripedLocks().stripeFor(i);
            seen[s] = true;
        }
        for (boolean b : seen) assertTrue(b, "all stripes should be hit");
    }

    @Test
    void testDrainOldest() {
        AccessOrderTracker<String> t = new AccessOrderTracker<>(4);
        t.touch("a");
        t.touch("b");
        t.touch("c");
        List<String> drained = new ArrayList<>();
        int n = t.drainOldest((k, v) -> drained.add(k));
        assertEquals(3, n);
        assertEquals(0, t.size());
        // 顺序：插入顺序
        assertEquals(List.of("a", "b", "c"), drained);
    }

    @Test
    void testClear() {
        AccessOrderTracker<String> t = new AccessOrderTracker<>(4);
        for (int i = 0; i < 100; i++) t.touch("k" + i);
        assertTrue(t.size() > 0);
        t.clear();
        assertEquals(0, t.size());
    }

    @Test
    void testStripedLocksPowerOf2() {
        assertThrows(IllegalArgumentException.class, () -> new StripedLocks(0));
        assertThrows(IllegalArgumentException.class, () -> new StripedLocks(3));
        assertThrows(IllegalArgumentException.class, () -> new StripedLocks(7));
        // 合法
        assertEquals(16, new StripedLocks(16).stripeCount());
        assertEquals(4, new StripedLocks(4).stripeCount());
    }

    @Test
    void testConcurrentAccess() throws Exception {
        AccessOrderTracker<Integer> t = new AccessOrderTracker<>(16);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            int threads = 8;
            int ops = 5_000;
            java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(threads);
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(threads);
            for (int th = 0; th < threads; th++) {
                final int seed = th;
                pool.submit(() -> {
                    try {
                        barrier.await();
                        for (int i = 0; i < ops; i++) {
                            int k = (seed * 17 + i) % 200;
                            t.touch(k);
                            if (i % 3 == 0) t.remove(k);
                        }
                    } catch (Exception e) {
                        // ignore
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertTrue(done.await(20, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
    }
}