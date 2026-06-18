package cn.wubo.cache;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class AsyncRefresherTest {

    @Test
    void testRefreshSuccessCounter() throws InterruptedException {
        AtomicInteger execCount = new AtomicInteger();
        java.util.concurrent.Executor exec = cmd -> {
            execCount.incrementAndGet();
            new Thread(cmd).start();
        };
        AsyncRefresher<String, String> refresher = new AsyncRefresher<>(exec);
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .executor(exec)
                .build();
        try {
            cache.put("k", "v1");
            refresher.refresh(cache, "k", k -> "v2");
            long deadline = System.currentTimeMillis() + 2_000;
            while (refresher.refreshSuccessCount() < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, refresher.refreshSuccessCount());
            assertEquals(0, refresher.refreshFailureCount());
            assertEquals("v2", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefreshFailureCounter() throws InterruptedException {
        java.util.concurrent.Executor exec = cmd -> new Thread(cmd).start();
        AsyncRefresher<String, String> refresher = new AsyncRefresher<>(exec);
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .executor(exec)
                .build();
        try {
            cache.put("k", "v1");
            refresher.refresh(cache, "k", k -> { throw new RuntimeException("boom"); });
            long deadline = System.currentTimeMillis() + 2_000;
            while (refresher.refreshFailureCount() < 1 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(1, refresher.refreshFailureCount());
            // 旧值仍在
            assertEquals("v1", cache.get("k"));
        } finally {
            cache.shutdown();
        }
    }

    @Test
    void testRefreshMultipleTimes() throws InterruptedException {
        java.util.concurrent.Executor exec = cmd -> new Thread(cmd).start();
        AsyncRefresher<String, String> refresher = new AsyncRefresher<>(exec);
        CHMCache<String, String> cache = CHMCache.<String, String>newBuilder()
                .maximumSize(10)
                .executor(exec)
                .build();
        try {
            cache.put("k", "v1");
            for (int i = 0; i < 5; i++) {
                final int n = i;
                refresher.refresh(cache, "k", k -> "v" + (n + 2));
            }
            long deadline = System.currentTimeMillis() + 2_000;
            while (refresher.refreshSuccessCount() < 5 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(5, refresher.refreshSuccessCount());
        } finally {
            cache.shutdown();
        }
    }
}