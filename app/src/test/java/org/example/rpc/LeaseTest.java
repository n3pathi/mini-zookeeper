package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Step 6 — Why We Need Consensus
 * <p>
 * Split-brain is the core problem: two nodes both think they are primary.
 * This test suite explores a lease-based approach to prevent it,
 * then shows why leases alone are not enough.
 */
@Timeout(15)
class LeaseTest {

    static final int PORT_A = 9097;
    static final int PORT_B = 9098;

    LeasedPrimaryKvServer nodeA;
    LeasedPrimaryKvServer nodeB;
    KvClient clientA;
    KvClient clientB;

    @AfterEach
    void tearDown() {
        if (clientA != null) try {
            clientA.close();
        } catch (Exception ignored) {
        }
        if (clientB != null) try {
            clientB.close();
        } catch (Exception ignored) {
        }
        if (nodeA != null) try {
            nodeA.close();
        } catch (Exception ignored) {
        }
        if (nodeB != null) try {
            nodeB.close();
        } catch (Exception ignored) {
        }
    }

    // -------------------------------------------------------------------------
    // Problem: split-brain without a lease
    // -------------------------------------------------------------------------

    /**
     * Two independently promoted primaries both accept writes.
     * Same key → different values on each node.
     * There is no way to know which is "correct" without external coordination.
     * <p>
     * This is the split-brain problem. Manual failover (Step 5) makes this easy
     * to trigger accidentally.
     */
    @Test
    void splitBrain_withoutLease_twoPrimaries_diverge() throws Exception {
        LeaseManager lm = new LeaseManager(60_000);

        // Both nodes started but neither holds the lease —
        // simulates the moment right after bad dual-promotion
        nodeA = new LeasedPrimaryKvServer(PORT_A, List.of(), "nodeA", lm);
        nodeB = new LeasedPrimaryKvServer(PORT_B, List.of(), "nodeB", lm);
        nodeA.start();
        nodeB.start();

        // Force both to act as leader (bypassing the lease)
        lm.forceLeader("nodeA");
        clientA = new SimpleKvClient("localhost", PORT_A);
        clientA.put("key", "from-nodeA");

        lm.forceLeader("nodeB");
        clientB = new SimpleKvClient("localhost", PORT_B);
        clientB.put("key", "from-nodeB");

        // Data has diverged — each node has a different value for the same key
        assertEquals(Optional.of("from-nodeA"), clientA.get("key"));
        assertEquals(Optional.of("from-nodeB"), clientB.get("key"));
        assertNotEquals(clientA.get("key"), clientB.get("key"));
    }

    // -------------------------------------------------------------------------
    // Solution attempt: lease-based leadership
    // -------------------------------------------------------------------------

    /**
     * With a lease, only the current lease holder accepts writes.
     * nodeB's write is silently rejected because it does not hold the lease.
     * Prevents split-brain as long as the lease manager is available.
     */
    @Test
    void withLease_onlyLeaseHolder_writesAreAccepted() throws Exception {
        LeaseManager lm = new LeaseManager(60_000);
        nodeA = new LeasedPrimaryKvServer(PORT_A, List.of(), "nodeA", lm);
        nodeB = new LeasedPrimaryKvServer(PORT_B, List.of(), "nodeB", lm);
        nodeA.start();
        nodeB.start();

        // Only nodeA acquires the lease
        assertTrue(lm.tryAcquire("nodeA"));

        clientA = new SimpleKvClient("localhost", PORT_A);
        clientB = new SimpleKvClient("localhost", PORT_B);

        clientA.put("key", "from-nodeA");  // accepted — nodeA is leader
        clientB.put("key", "from-nodeB");  // rejected — nodeB has no lease

        assertEquals(Optional.of("from-nodeA"), clientA.get("key"));
        assertEquals(Optional.empty(), clientB.get("key")); // write was rejected
    }

    /**
     * When nodeA's lease expires (it stopped renewing — simulates a pause or crash),
     * nodeA's writes are rejected. nodeB acquires the new lease and can write.
     * No split-brain: there is never a moment where both accept writes.
     */
    @Test
    void leaseExpiry_preventsOldPrimary_fromWriting() throws Exception {
        LeaseManager lm = new LeaseManager(200); // very short TTL: 200ms
        nodeA = new LeasedPrimaryKvServer(PORT_A, List.of(), "nodeA", lm);
        nodeB = new LeasedPrimaryKvServer(PORT_B, List.of(), "nodeB", lm);
        nodeA.start();
        nodeB.start();

        assertTrue(lm.tryAcquire("nodeA"));

        clientA = new SimpleKvClient("localhost", PORT_A);
        clientA.put("k1", "v1"); // accepted — lease is valid

        Thread.sleep(300); // lease expires (nodeA stopped renewing)

        clientA.put("k2", "v2"); // rejected — lease expired

        assertEquals(Optional.of("v1"), clientA.get("k1")); // written before expiry
        assertEquals(Optional.empty(), clientA.get("k2")); // rejected after expiry

        // nodeB takes over
        assertTrue(lm.tryAcquire("nodeB"));
        clientB = new SimpleKvClient("localhost", PORT_B);
        clientB.put("k3", "v3");
        assertEquals(Optional.of("v3"), clientB.get("k3")); // nodeB is now leader
    }

    /**
     * A node that holds the lease can renew it before it expires.
     * This models the heartbeat a healthy primary sends to the lease manager.
     */
    @Test
    void renewLease_extendsLeadership() throws Exception {
        LeaseManager lm = new LeaseManager(200);
        nodeA = new LeasedPrimaryKvServer(PORT_A, List.of(), "nodeA", lm);
        nodeA.start();
        clientA = new SimpleKvClient("localhost", PORT_A);

        assertTrue(lm.tryAcquire("nodeA"));

        // Keep renewing — nodeA stays leader across multiple TTL windows
        for (int i = 0; i < 5; i++) {
            Thread.sleep(100); // half a TTL
            assertTrue(lm.renew("nodeA"), "renewal should succeed while lease is active");
        }

        clientA.put("key", "value");
        assertEquals(Optional.of("value"), clientA.get("key"));
    }

    // -------------------------------------------------------------------------
    // The remaining problem: lease manager is a single point of failure
    // -------------------------------------------------------------------------

    /**
     * If the lease manager itself crashes, no node can acquire or renew a lease.
     * The system becomes unavailable — neither node can accept writes.
     * <p>
     * The fix is to replicate the lease manager itself across multiple nodes
     * so that it survives failures. That requires those nodes to agree on
     * who holds the lease — which requires CONSENSUS (Raft).
     * <p>
     * This is why Phase 3 exists.
     */
    @Test
    void leaseManager_crash_systemBecomesUnavailable() throws Exception {
        LeaseManager lm = new LeaseManager(60_000);
        assertTrue(lm.tryAcquire("nodeA"));
        assertTrue(lm.isLeader("nodeA"));

        lm.close(); // lease manager crashes

        assertFalse(lm.isLeader("nodeA")); // existing holder cannot verify leadership
        assertFalse(lm.renew("nodeA"));    // renewal fails — manager is gone
        assertFalse(lm.tryAcquire("nodeB")); // promotion fails — manager is gone

        // System is now stuck: no writes can be safely accepted.
        // To fix this, the lease manager itself needs to be fault-tolerant.
        // A fault-tolerant, replicated lease manager IS Raft.
    }
}
