package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(15)
class FailoverTest {

    // Ports
    static final int PRIMARY_PORT = 9094;
    static final int REPLICA1_PORT = 9095;

    KvServer replica1;
    PrimaryKvServer primary;
    FailoverKvClient client;

    @AfterEach
    void tearDown() {
        if (client != null) try {
            client.close();
        } catch (Exception ignored) {
        }
        if (primary != null) try {
            primary.close();
        } catch (Exception ignored) {
        }
        if (replica1 != null) try {
            replica1.close();
        } catch (Exception ignored) {
        }
    }

    // --- Test 1: normal operation ---
    @Test
    void failoverClient_worksNormally_withHealthyPrimary() throws Exception {
        replica1 = new KvServer(REPLICA1_PORT);
        replica1.start();
        primary = new PrimaryKvServer(PRIMARY_PORT,
                List.of("localhost:" + REPLICA1_PORT));
        primary.start();

        client = new FailoverKvClient(
                List.of("localhost:" + PRIMARY_PORT));

        client.put("k", "v");
        assertEquals(Optional.of("v"), client.get("k"));

        client.close();
        primary.close();
        replica1.close();
    }

    // --- Test 2: failover to next endpoint when primary is down ---
    @Test
    void failoverClient_retriesNextEndpoint_whenPrimaryDown() throws Exception {
        replica1 = new KvServer(REPLICA1_PORT);
        replica1.start();
        primary = new PrimaryKvServer(PRIMARY_PORT,
                List.of("localhost:" + REPLICA1_PORT));
        primary.start();

        // Pre-populate via primary
        client = new FailoverKvClient(
                List.of("localhost:" + PRIMARY_PORT,
                        "localhost:" + REPLICA1_PORT));
        client.put("k", "v");

        // Kill primary — client should failover to replica1
        primary.close();
        Thread.sleep(100);

        // Promote replica1 to primary manually
        KvServer oldReplica1 = replica1;
        oldReplica1.close();
        Thread.sleep(100);
        // Note: we close old replica and reopen on same port
        PrimaryKvServer newPrimary = new PrimaryKvServer(REPLICA1_PORT, List.of());
        newPrimary.start();

        // Client retries — should succeed against new primary on REPLICA1_PORT
        client.put("k2", "v2");
        assertEquals(Optional.of("v2"), client.get("k2"));

        client.close();
        newPrimary.close();
    }

    // --- Test 3: all endpoints down → throws ---
    @Test
    void failoverClient_throwsRuntimeException_whenAllEndpointsDown() {
        // Neither port has a server running
        client = new FailoverKvClient(
                List.of("localhost:" + PRIMARY_PORT,
                        "localhost:" + REPLICA1_PORT));

        assertThrows(RuntimeException.class, () -> client.put("k", "v"));
        client.close();
    }

    // --- Test 4: split-brain — two primaries diverge ---
    @Test
    void splitBrain_twoActivePrimaries_diverge() throws Exception {
        // Start primary1 with no replicas (isolated)
        PrimaryKvServer primary1 = new PrimaryKvServer(PRIMARY_PORT, List.of());
        primary1.start();

        // Start primary2 on replica port (simulates bad promotion of isolated node)
        PrimaryKvServer primary2 = new PrimaryKvServer(REPLICA1_PORT, List.of());
        primary2.start();

        KvClient client1 = new SimpleKvClient("localhost", PRIMARY_PORT);
        KvClient client2 = new SimpleKvClient("localhost", REPLICA1_PORT);

        // Each client writes to its own "primary"
        client1.put("key", "from-primary1");
        client2.put("key", "from-primary2");

        // Both accept writes — data has diverged
        assertEquals(Optional.of("from-primary1"), client1.get("key"));
        assertEquals(Optional.of("from-primary2"), client2.get("key"));

        // No automatic resolution — this is the split-brain problem
        // A client connecting to either node gets different data for the same key
        assertNotEquals(client1.get("key"), client2.get("key"));

        client1.close();
        client2.close();
        primary1.close();
        primary2.close();
    }

    // --- Test 5: failover is fast — no long timeout ---
    @Test
    void failoverClient_failsOverQuickly() throws Exception {
        replica1 = new KvServer(REPLICA1_PORT);
        replica1.start();
        primary = new PrimaryKvServer(PRIMARY_PORT,
                List.of("localhost:" + REPLICA1_PORT));
        primary.start();
        primary.close();
        Thread.sleep(100);

        client = new FailoverKvClient(
                List.of("localhost:" + PRIMARY_PORT,
                        "localhost:" + REPLICA1_PORT));

        client.put("k", "v");
        long start = System.currentTimeMillis();
        // This should fail fast on primary, then succeed on replica1
        // replica1 still has the old data (it was a replica)
        assertDoesNotThrow(() -> client.get("k"));
        long elapsed = System.currentTimeMillis() - start;

        assertTrue(elapsed < 2000, "Failover took too long: " + elapsed + "ms");

        client.close();
        replica1.close();
    }
}
