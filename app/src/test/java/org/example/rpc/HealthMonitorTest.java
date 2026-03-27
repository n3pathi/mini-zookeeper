package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@Timeout(15)
class HealthMonitorTest {

    // Fast tick settings for testing: 100ms tick, suspect after 3 missed ticks = 300ms timeout
    private static final long TICK_MS       = 100;
    private static final int  MAX_MISSED    = 3;

    private HealthMonitor monitor;

    @BeforeEach
    void setUp() {
        monitor = new HealthMonitor(TICK_MS, MAX_MISSED);
    }

    @AfterEach
    void tearDown() {
        monitor.close();
    }

    // -------------------------------------------------------------------------
    // HealthMonitor — basic status
    // -------------------------------------------------------------------------

    @Test
    void newlyRegisteredNode_isHealthy() {
        monitor.register("node-1");
        // A node is considered healthy immediately after registering
        // (it hasn't had a chance to miss any heartbeats yet)
        assertEquals(NodeStatus.HEALTHY, monitor.getStatus("node-1"));
    }

    @Test
    void nodeAfterHeartbeat_isHealthy() {
        monitor.register("node-1");
        monitor.heartbeat("node-1");
        assertEquals(NodeStatus.HEALTHY, monitor.getStatus("node-1"));
    }

    @Test
    void node_isSuspectedDown_afterMissingHeartbeats() throws InterruptedException {
        monitor.register("node-1");
        monitor.heartbeat("node-1");

        // Wait longer than tickMs * maxMissed
        Thread.sleep(TICK_MS * MAX_MISSED + TICK_MS);

        assertEquals(NodeStatus.SUSPECTED_DOWN, monitor.getStatus("node-1"));
    }

    @Test
    void node_recovers_afterSendingHeartbeatAgain() throws InterruptedException {
        monitor.register("node-1");
        monitor.heartbeat("node-1");

        // Let it go stale
        Thread.sleep(TICK_MS * MAX_MISSED + TICK_MS);
        assertEquals(NodeStatus.SUSPECTED_DOWN, monitor.getStatus("node-1"));

        // Send a heartbeat — should recover immediately
        monitor.heartbeat("node-1");
        assertEquals(NodeStatus.HEALTHY, monitor.getStatus("node-1"));
    }

    @Test
    void multipleNodes_trackedIndependently() throws InterruptedException {
        monitor.register("node-1");
        monitor.register("node-2");

        monitor.heartbeat("node-1");
        monitor.heartbeat("node-2");

        // Only node-2 keeps sending heartbeats
        Thread.sleep(TICK_MS * MAX_MISSED + TICK_MS);
        monitor.heartbeat("node-2");

        assertEquals(NodeStatus.SUSPECTED_DOWN, monitor.getStatus("node-1"));
        assertEquals(NodeStatus.HEALTHY, monitor.getStatus("node-2"));
    }

    @Test
    void getStatus_unknownNode_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> monitor.getStatus("ghost"));
    }

    @Test
    void heartbeat_unknownNode_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> monitor.heartbeat("ghost"));
    }

    // -------------------------------------------------------------------------
    // HeartbeatSender
    // -------------------------------------------------------------------------

    @Test
    void heartbeatSender_keepsNodeHealthy() throws InterruptedException {
        monitor.register("node-1");

        // Sender fires every 50ms — well within the 300ms timeout
        HeartbeatSender sender = new HeartbeatSender("node-1", monitor, 50);

        // Wait two full timeout windows — node should still be healthy
        Thread.sleep(TICK_MS * MAX_MISSED * 2);
        assertEquals(NodeStatus.HEALTHY, monitor.getStatus("node-1"));

        sender.close();
    }

    @Test
    void heartbeatSender_afterClose_nodeEventuallyBecomesStale() throws InterruptedException {
        monitor.register("node-1");
        HeartbeatSender sender = new HeartbeatSender("node-1", monitor, 50);

        // Let it run, confirm healthy
        Thread.sleep(100);
        assertEquals(NodeStatus.HEALTHY, monitor.getStatus("node-1"));

        // Stop the sender — no more heartbeats
        sender.close();

        // Wait for node to go stale
        Thread.sleep(TICK_MS * MAX_MISSED + TICK_MS);
        assertEquals(NodeStatus.SUSPECTED_DOWN, monitor.getStatus("node-1"));
    }

    // -------------------------------------------------------------------------
    // Integration: PrimaryKvServer skips SUSPECTED_DOWN replicas
    // -------------------------------------------------------------------------

    @Test
    void primary_skipsSuspectedDownReplica_withoutTimingOut() throws Exception {
        int primaryPort  = 9091;
        int replica1Port = 9092;
        int replica2Port = 9093;

        KvServer replica1 = new KvServer(replica1Port);
        replica1.start();

        KvServer replica2 = new KvServer(replica2Port);
        replica2.start();

        // Register both replicas in monitor; only replica2 sends heartbeats
        monitor.register("localhost:" + replica1Port);
        monitor.register("localhost:" + replica2Port);

        HeartbeatSender sender2 = new HeartbeatSender(
                "localhost:" + replica2Port, monitor, 50);

        // Let replica1 go stale (no sender for it)
        Thread.sleep(TICK_MS * MAX_MISSED + TICK_MS);
        assertEquals(NodeStatus.SUSPECTED_DOWN,
                monitor.getStatus("localhost:" + replica1Port));

        // Primary knows about the monitor — should skip replica1 without timing out
        PrimaryKvServer primary = new PrimaryKvServer(primaryPort,
                List.of("localhost:" + replica1Port, "localhost:" + replica2Port));
        primary.start();

        KvClient client        = new SimpleKvClient("localhost", primaryPort);
        KvClient replicaClient2 = new SimpleKvClient("localhost", replica2Port);

        long start = System.currentTimeMillis();
        client.put("key", "value");
        long elapsed = System.currentTimeMillis() - start;

        // Must complete fast — no timeout waiting for dead replica1
        assertTrue(elapsed < 1000, "put took too long: " + elapsed + "ms — primary didn't skip dead replica");

        // replica2 must have received the write
        assertEquals(Optional.of("value"), replicaClient2.get("key"));

        client.close();
        replicaClient2.close();
        primary.close();
        sender2.close();
        replica1.close();
        replica2.close();
    }
}
