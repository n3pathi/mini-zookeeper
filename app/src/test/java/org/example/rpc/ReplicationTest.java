package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class ReplicationTest {

    private static final int PRIMARY_PORT  = 9085;
    private static final int REPLICA1_PORT = 9086;
    private static final int REPLICA2_PORT = 9087;

    private KvServer replica1;
    private KvServer replica2;
    private PrimaryKvServer primary;

    private KvClient client;          // talks to primary
    private KvClient replicaClient1;  // reads directly from replica1
    private KvClient replicaClient2;  // reads directly from replica2

    @BeforeEach
    void setUp() throws Exception {
        replica1 = new KvServer(REPLICA1_PORT);
        replica1.start();

        replica2 = new KvServer(REPLICA2_PORT);
        replica2.start();

        primary = new PrimaryKvServer(PRIMARY_PORT,
                List.of("localhost:" + REPLICA1_PORT, "localhost:" + REPLICA2_PORT));
        primary.start();

        client        = new SimpleKvClient("localhost", PRIMARY_PORT);
        replicaClient1 = new SimpleKvClient("localhost", REPLICA1_PORT);
        replicaClient2 = new SimpleKvClient("localhost", REPLICA2_PORT);
    }

    @AfterEach
    void tearDown() throws Exception {
        client.close();
        replicaClient1.close();
        replicaClient2.close();
        primary.close();
        replica1.close();
        replica2.close();
    }

    // -------------------------------------------------------------------------
    // Basic replication
    // -------------------------------------------------------------------------

    @Test
    void put_replicatesToBothReplicas() {
        client.put("key", "value");

        assertEquals(Optional.of("value"), replicaClient1.get("key"));
        assertEquals(Optional.of("value"), replicaClient2.get("key"));
    }

    @Test
    void delete_replicatesToBothReplicas() {
        client.put("key", "value");
        client.delete("key");

        assertEquals(Optional.empty(), replicaClient1.get("key"));
        assertEquals(Optional.empty(), replicaClient2.get("key"));
    }

    @Test
    void get_isServedLocally_doesNotRequireReplicas() {
        // Write directly to primary's store via put, then read back
        // Read should succeed even if replicas are absent
        client.put("local", "only");
        assertEquals(Optional.of("only"), client.get("local"));
    }

    @Test
    void multipleWrites_allReplicateCorrectly() {
        for (int i = 0; i < 20; i++) {
            client.put("key-" + i, "val-" + i);
        }

        for (int i = 0; i < 20; i++) {
            assertEquals(Optional.of("val-" + i), replicaClient1.get("key-" + i));
            assertEquals(Optional.of("val-" + i), replicaClient2.get("key-" + i));
        }
    }

    @Test
    void overwrite_replicatesNewValue() {
        client.put("key", "v1");
        client.put("key", "v2");

        assertEquals(Optional.of("v2"), replicaClient1.get("key"));
        assertEquals(Optional.of("v2"), replicaClient2.get("key"));
    }

    // -------------------------------------------------------------------------
    // Degraded mode — replica down
    // -------------------------------------------------------------------------

    @Test
    void primaryContinuesServing_whenOneReplicaIsDown() throws Exception {
        replica1.close();
        replicaClient1.close();

        // Primary must still accept writes without throwing
        assertDoesNotThrow(() -> client.put("key", "value"));
        assertEquals(Optional.of("value"), client.get("key"));

        // replica2 (still alive) must have the write
        assertEquals(Optional.of("value"), replicaClient2.get("key"));
    }

    @Test
    void primaryContinuesServing_whenAllReplicasAreDown() throws Exception {
        replica1.close();
        replica2.close();
        replicaClient1.close();
        replicaClient2.close();

        // Primary must still serve reads and writes locally
        assertDoesNotThrow(() -> client.put("key", "value"));
        assertEquals(Optional.of("value"), client.get("key"));
    }

    @Test
    void primaryReturnsOk_beforeReplicaFailureIsDetected() throws Exception {
        // Close replica2 silently — primary doesn't know yet
        replica2.close();
        replicaClient2.close();

        // The first write will discover replica2 is down — must not throw to client
        assertDoesNotThrow(() -> {
            client.put("resilient-key", "yes");
        });
    }

    // -------------------------------------------------------------------------
    // Concurrency
    // -------------------------------------------------------------------------

    @Test
    void concurrentWrites_allReplicateToReplica1() throws InterruptedException {
        int threadCount = 10;
        CountDownLatch done = new CountDownLatch(threadCount);
        AtomicInteger errors = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int id = t;
            Thread.ofVirtual().start(() -> {
                try {
                    client.put("concurrent-" + id, "val-" + id);
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    done.countDown();
                }
            });
        }

        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertEquals(0, errors.get(), "No writes should fail under concurrency");

        for (int t = 0; t < threadCount; t++) {
            assertEquals(Optional.of("val-" + t), replicaClient1.get("concurrent-" + t));
        }
    }
}
