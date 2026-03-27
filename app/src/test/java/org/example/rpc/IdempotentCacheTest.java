package org.example.rpc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for IdempotentCache<T> — a TTL-based idempotency cache.
 *
 * Design contract (derived from Resilience4j Cache lessons):
 *   - computeIfAbsent() is the primary API — atomic check-then-execute
 *   - get() is a read-only peek (does NOT execute the supplier)
 *   - Entries expire after ttlSeconds and are not returned after expiry
 *   - Background cleaner evicts stale entries periodically
 *   - close() shuts down the background thread cleanly
 */
class IdempotentCacheTest {

    private IdempotentCache<String> cache;

    @BeforeEach
    void setUp() {
        // 2-second TTL for tests that check expiry
        cache = new IdempotentCache<>(2);
    }

    // -------------------------------------------------------------------------
    // Basic get / put behaviour
    // -------------------------------------------------------------------------

    @Test
    void get_returnsEmpty_whenKeyNotPresent() {
        assertTrue(cache.get("unknown").isEmpty());
    }

    @Test
    void computeIfAbsent_executesSupplier_onFirstCall() {
        String result = cache.computeIfAbsent("req-1", () -> "response-A");
        assertEquals("response-A", result);
    }

    @Test
    void computeIfAbsent_returnsCache_onSecondCall_withoutExecutingSupplier() {
        AtomicInteger callCount = new AtomicInteger(0);

        cache.computeIfAbsent("req-1", () -> {
            callCount.incrementAndGet();
            return "response-A";
        });
        cache.computeIfAbsent("req-1", () -> {
            callCount.incrementAndGet();
            return "response-B"; // should never be called
        });

        // Supplier must only have been called once
        assertEquals(1, callCount.get());
    }

    @Test
    void get_returnsValue_afterComputeIfAbsent() {
        cache.computeIfAbsent("req-1", () -> "response-A");
        Optional<String> result = cache.get("req-1");
        assertTrue(result.isPresent());
        assertEquals("response-A", result.get());
    }

    @Test
    void differentKeys_areStoredIndependently() {
        cache.computeIfAbsent("req-1", () -> "response-A");
        cache.computeIfAbsent("req-2", () -> "response-B");

        assertEquals("response-A", cache.get("req-1").orElseThrow());
        assertEquals("response-B", cache.get("req-2").orElseThrow());
    }

    // -------------------------------------------------------------------------
    // TTL / expiry behaviour
    // -------------------------------------------------------------------------

    @Test
    void get_returnsEmpty_afterTtlExpires() throws InterruptedException {
        cache.computeIfAbsent("req-1", () -> "response-A");

        // Wait for TTL to expire (TTL = 2s, wait 2.5s)
        Thread.sleep(2500);

        assertTrue(cache.get("req-1").isEmpty(),
            "Entry should be expired after TTL");
    }

    @Test
    void computeIfAbsent_executesSupplierAgain_afterTtlExpires() throws InterruptedException {
        AtomicInteger callCount = new AtomicInteger(0);

        cache.computeIfAbsent("req-1", () -> {
            callCount.incrementAndGet();
            return "first";
        });

        Thread.sleep(2500); // wait for TTL

        cache.computeIfAbsent("req-1", () -> {
            callCount.incrementAndGet();
            return "second";
        });

        // Should have been called twice — once before TTL, once after
        assertEquals(2, callCount.get());
        assertEquals("second", cache.get("req-1").orElseThrow());
    }

    // -------------------------------------------------------------------------
    // Concurrency — the key lesson from Resilience4j
    // -------------------------------------------------------------------------

    @Test
    void computeIfAbsent_isAtomic_underConcurrentAccess() throws InterruptedException {
        int threadCount = 20;
        AtomicInteger supplierCallCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);

        var executor = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await(); // all threads start at the same time
                    cache.computeIfAbsent("shared-key", () -> {
                        supplierCallCount.incrementAndGet();
                        return "result";
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // release all threads simultaneously
        doneLatch.await();
        executor.shutdown();

        // The supplier must have been called exactly once despite 20 concurrent callers
        assertEquals(1, supplierCallCount.get(),
            "Supplier must be called exactly once — computeIfAbsent must be atomic");
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    @Test
    void close_doesNotThrow() {
        assertDoesNotThrow(() -> cache.close());
    }

    @Test
    void close_isIdempotent() {
        assertDoesNotThrow(() -> {
            cache.close();
            cache.close(); // second close must not throw
        });
    }
}
