package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(10)
class KvStoreTest {

    private static final int PORT = 9083;

    private KvServer server;
    private KvClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = new KvServer(PORT);
        server.start();
        client = new SimpleKvClient("localhost", PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        server.close();
    }

    // -------------------------------------------------------------------------
    // Basic operations
    // -------------------------------------------------------------------------

    @Test
    void get_returnsEmpty_whenKeyDoesNotExist() {
        assertEquals(Optional.empty(), client.get("missing"));
    }

    @Test
    void put_thenGet_returnsValue() {
        client.put("name", "alice");
        assertEquals(Optional.of("alice"), client.get("name"));
    }

    @Test
    void put_overwritesExistingValue() {
        client.put("key", "v1");
        client.put("key", "v2");
        assertEquals(Optional.of("v2"), client.get("key"));
    }

    @Test
    void delete_removesKey() {
        client.put("key", "value");
        client.delete("key");
        assertEquals(Optional.empty(), client.get("key"));
    }

    @Test
    void delete_nonExistentKey_doesNotThrow() {
        assertDoesNotThrow(() -> client.delete("ghost"));
    }

    @Test
    void multipleKeys_areStoredIndependently() {
        client.put("a", "1");
        client.put("b", "2");
        client.put("c", "3");
        assertEquals(Optional.of("1"), client.get("a"));
        assertEquals(Optional.of("2"), client.get("b"));
        assertEquals(Optional.of("3"), client.get("c"));
    }

    @Test
    void put_withEmptyValue_isAllowed() {
        client.put("key", "");
        assertEquals(Optional.of(""), client.get("key"));
    }

    @Test
    void put_withSpecialCharacters_roundTrips() {
        String value = "hello/world:foo=bar&baz";
        client.put("key", value);
        assertEquals(Optional.of(value), client.get("key"));
    }

    // -------------------------------------------------------------------------
    // Concurrency
    // -------------------------------------------------------------------------

    @Test
    void concurrentPuts_toDistinctKeys_allSucceed() throws InterruptedException {
        int threadCount = 20;
        int keysPerThread = 50;
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            pool.submit(() -> {
                for (int i = 0; i < keysPerThread; i++) {
                    client.put("thread-" + threadId + "-key-" + i, "val-" + i);
                }
                latch.countDown();
            });
        }

        assertTrue(latch.await(8, TimeUnit.SECONDS));
        pool.shutdown();

        // Verify a sample of keys
        for (int t = 0; t < threadCount; t++) {
            assertEquals(Optional.of("val-0"), client.get("thread-" + t + "-key-0"));
            assertEquals(Optional.of("val-" + (keysPerThread - 1)),
                    client.get("thread-" + t + "-key-" + (keysPerThread - 1)));
        }
    }

    @Test
    void concurrentPuts_toSameKey_lastWriteWins() throws InterruptedException {
        int threadCount = 20;
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final String value = "thread-" + t;
            Thread.ofVirtual().start(() -> {
                ready.countDown();
                try { start.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                client.put("shared-key", value);
                successCount.incrementAndGet();
                done.countDown();
            });
        }

        ready.await();
        start.countDown();
        assertTrue(done.await(8, TimeUnit.SECONDS));

        // All 20 puts must succeed without throwing
        assertEquals(threadCount, successCount.get());
        // Key must exist with some value (exactly one writer wins)
        assertTrue(client.get("shared-key").isPresent());
    }

    @Test
    void concurrentPutsAndGets_doNotThrow() throws InterruptedException {
        // Pre-populate
        for (int i = 0; i < 10; i++) {
            client.put("key-" + i, "init");
        }

        int threadCount = 20;
        CountDownLatch done = new CountDownLatch(threadCount);
        List<Throwable> errors = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Thread.ofVirtual().start(() -> {
                try {
                    if (threadId % 2 == 0) {
                        // writers
                        for (int i = 0; i < 10; i++) {
                            client.put("key-" + i, "v-" + threadId);
                        }
                    } else {
                        // readers
                        for (int i = 0; i < 10; i++) {
                            client.get("key-" + i);
                        }
                    }
                } catch (Throwable e) {
                    synchronized (errors) { errors.add(e); }
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(8, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "Unexpected errors: " + errors);
    }
}
