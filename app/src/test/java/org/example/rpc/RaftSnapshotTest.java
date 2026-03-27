package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3, Step 4 — Log Compaction + Snapshotting
 *
 * Without compaction the Raft log grows without bound. A snapshot captures
 * the full state machine at a committed index and lets the node discard all
 * log entries up to that point.
 *
 * When a follower is so far behind that the leader has already discarded the
 * log entries it needs, the leader sends an {@code InstallSnapshot} RPC instead
 * of {@code AppendEntries}. The follower installs the snapshot, then receives
 * any new entries that followed it.
 *
 * <h3>New API required on RaftNode</h3>
 * <pre>
 *   void takeSnapshot()
 *       — Capture the current state machine at {@code commitIndex}.
 *         Record snapshotLastIndex = commitIndex and
 *         snapshotLastTerm = term of the last discarded entry.
 *         Discard all in-memory log entries with index {@code <= commitIndex}.
 *         No-op if commitIndex == 0 (nothing committed yet).
 *
 *   int getLogSize()
 *       — Return the number of entries currently in the in-memory log.
 *         After compaction this is smaller than the total entries ever appended.
 * </pre>
 *
 * <h3>New classes</h3>
 * <ul>
 *   <li>{@code Snapshot} — {@code record(int lastIncludedIndex, int lastIncludedTerm,
 *       Map<String,String> data)} — the serializable snapshot payload.
 *   <li>{@code InstallSnapshotRequest} — RPC payload sent leader → follower:
 *       {@code (int term, String leaderId, int lastIncludedIndex, int lastIncludedTerm,
 *       List<String> data)} where {@code data} is the state machine encoded as
 *       {@code "key:value"} strings (one entry per map entry).
 *   <li>{@code InstallSnapshotResponse} — {@code record(int term)} — follower's reply.
 * </ul>
 *
 * <h3>Key implementation changes in RaftNode</h3>
 * <ul>
 *   <li>Add {@code int snapshotLastIndex = 0} and {@code int snapshotLastTerm = 0}.
 *   <li><strong>Log index arithmetic:</strong> after compaction the first entry in
 *       {@code log} has global index {@code snapshotLastIndex + 1}.
 *       <pre>
 *   global index i → log.get(i - snapshotLastIndex - 1)
 *   last global log index = snapshotLastIndex + log.size()
 *       </pre>
 *   <li><strong>{@code getMyLastLogTerm()}:</strong> when {@code log} is empty return
 *       {@code snapshotLastTerm} (not 0) so the election log-freshness check is correct
 *       after compaction.
 *   <li><strong>{@code write()}:</strong> new entry index = {@code snapshotLastIndex + log.size() + 1}.
 *   <li><strong>{@code becomeLeader()}:</strong> {@code ni = snapshotLastIndex + log.size() + 1}.
 *   <li><strong>{@code handleSuccessfulReplication()}:</strong> iterate from
 *       {@code snapshotLastIndex + log.size()} down to {@code commitIndex + 1}.
 *   <li><strong>Heartbeat task — send snapshot when follower is too far behind:</strong>
 *       <pre>
 *   if (nextIndex[peer] {@code <=} snapshotLastIndex) {
 *       send InstallSnapshot(currentTerm, nodeId,
 *                            snapshotLastIndex, snapshotLastTerm,
 *                            encode(stateMachine));
 *       // on success: matchIndex[peer] = snapshotLastIndex,
 *       //             nextIndex[peer]  = snapshotLastIndex + 1
 *   }
 *       </pre>
 *   <li><strong>{@code handleInstallSnapshot}:</strong>
 *       <ol>
 *         <li>If {@code req.term() < currentTerm} reply with current term and return.
 *         <li>Call {@code stepDownIfTermIsHigher(req.term())} and reset election timer.
 *         <li>If {@code req.lastIncludedIndex() <= commitIndex} the snapshot is stale
 *             — reply success and return (we already have this data).
 *         <li>Replace {@code stateMachine} with decoded snapshot data.
 *         <li>Set {@code snapshotLastIndex = req.lastIncludedIndex()},
 *             {@code snapshotLastTerm = req.lastIncludedTerm()}.
 *         <li>Set {@code commitIndex = lastApplied = snapshotLastIndex}.
 *         <li>Truncate {@code log}: remove all entries with global index
 *             {@code <= snapshotLastIndex}.
 *       </ol>
 *   <li><strong>{@code handleAppendEntries} — consistency check with snapshot boundary:</strong>
 *       <pre>
 *   if (prevLogIndex == snapshotLastIndex) {
 *       // compare against snapshotLastTerm, not a log entry
 *       termMatch = (prevLogTerm == snapshotLastTerm);
 *   } else if (prevLogIndex > snapshotLastIndex) {
 *       // entry must be in the in-memory log
 *       termMatch = log.get(prevLogIndex - snapshotLastIndex - 1).term() == prevLogTerm;
 *   } else {
 *       // prevLogIndex < snapshotLastIndex — already covered by snapshot, accept
 *       termMatch = true;
 *   }
 *       </pre>
 *   <li>Register a new RPC handler {@code "installSnapshot"} in {@code start()}.
 * </ul>
 */
@Timeout(30)
class RaftSnapshotTest {

    // Ports 7301–7303: reserved for snapshot tests.
    static final int BASE_PORT = 7301;

    List<RaftNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(node -> {
            try { node.stop(); } catch (Exception ignored) {}
        });
        nodes.clear();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private List<String> addresses(int n) {
        List<String> addrs = new ArrayList<>();
        for (int i = 0; i < n; i++) addrs.add("localhost:" + (BASE_PORT + i));
        return addrs;
    }

    private List<RaftNode> cluster(int n) {
        List<String> addrs = addresses(n);
        for (int i = 0; i < n; i++) {
            List<String> peers = new ArrayList<>(addrs);
            peers.remove(i);
            RaftNode node = new RaftNode("node-" + i, BASE_PORT + i, peers);
            nodes.add(node);
        }
        return nodes;
    }

    private RaftNode waitForLeader(List<RaftNode> candidates, int timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<RaftNode> leaders = candidates.stream()
                    .filter(n -> n.getState() == RaftState.LEADER)
                    .toList();
            if (leaders.size() == 1) return leaders.getFirst();
            Thread.sleep(50);
        }
        long count = candidates.stream()
                .filter(n -> n.getState() == RaftState.LEADER).count();
        throw new AssertionError(
                "Expected exactly 1 leader within " + timeoutMs + "ms, got " + count);
    }

    private void partition(RaftNode a, RaftNode b) {
        a.dropMessagesTo("localhost:" + b.getPort());
        b.dropMessagesTo("localhost:" + a.getPort());
    }

    private void heal(RaftNode a, RaftNode b) {
        a.restoreMessagesTo("localhost:" + b.getPort());
        b.restoreMessagesTo("localhost:" + a.getPort());
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /**
     * After committing N entries and calling {@code takeSnapshot()}, the
     * in-memory log must be empty while the state machine retains all values.
     *
     * <p>This is the most basic invariant of log compaction: a snapshot at
     * {@code commitIndex} makes every prior log entry redundant — the state
     * machine already reflects them.
     */
    @Test
    void snapshot_compactsLog() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        leader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        leader.write("put:k3:v3").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // let followers apply

        assertEquals(3, leader.getLogSize(), "log must have 3 entries before snapshot");

        leader.takeSnapshot();

        assertEquals(0, leader.getLogSize(),
                "log must be empty after snapshot — all committed entries compacted");
        assertEquals(Optional.of("v1"), leader.read("k1"), "state machine must retain k1");
        assertEquals(Optional.of("v2"), leader.read("k2"), "state machine must retain k2");
        assertEquals(Optional.of("v3"), leader.read("k3"), "state machine must retain k3");
    }

    /**
     * Entries appended after a snapshot must still be replicated and committed
     * correctly across all followers.
     *
     * <p>After {@code takeSnapshot()}, {@code snapshotLastIndex} is the new
     * log offset. All subsequent index arithmetic (prevLogIndex, write index,
     * commit check) must account for this offset. This test validates that the
     * offset is applied consistently on both leader and followers.
     */
    @Test
    void entriesAfterSnapshot_replicatedAndCommittedCorrectly() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        // Commit 3 entries, then compact.
        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        leader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        leader.write("put:k3:v3").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // allow followers to apply

        leader.takeSnapshot();
        assertEquals(0, leader.getLogSize(), "log must be empty after snapshot");

        // Commit 2 more entries — index arithmetic must use the snapshot offset.
        leader.write("put:k4:v4").get(2, TimeUnit.SECONDS);
        leader.write("put:k5:v5").get(2, TimeUnit.SECONDS);
        Thread.sleep(500); // allow followers to apply

        for (RaftNode node : nodes) {
            assertEquals(Optional.of("v4"), node.read("k4"),
                    node.getNodeId() + ": k4 must be present after post-snapshot replication");
            assertEquals(Optional.of("v5"), node.read("k5"),
                    node.getNodeId() + ": k5 must be present after post-snapshot replication");
        }
        assertEquals(2, leader.getLogSize(),
                "log must hold only the 2 post-snapshot entries");
    }

    /**
     * A follower partitioned while the leader committed and compacted entries
     * must receive an {@code InstallSnapshot} RPC when the partition heals —
     * because the log entries it missed no longer exist on the leader.
     *
     * <p>After installing the snapshot the follower must reflect the full state
     * machine as of the snapshot boundary, including entries that were committed
     * before the partition.
     */
    @Test
    void laggingFollower_receivesSnapshot_andCatchesUp() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);
        RaftNode laggingFollower = nodes.stream()
                .filter(n -> n != leader)
                .findFirst().orElseThrow();
        RaftNode otherFollower = nodes.stream()
                .filter(n -> n != leader && n != laggingFollower)
                .findFirst().orElseThrow();

        // Commit k1 while all nodes are healthy.
        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        Thread.sleep(300);

        // Partition the lagging follower from both the leader and the other follower.
        partition(laggingFollower, leader);
        partition(laggingFollower, otherFollower);

        // Commit k2 and k3 — leader + otherFollower form the quorum.
        leader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        leader.write("put:k3:v3").get(2, TimeUnit.SECONDS);

        // Compact: the lagging follower is now behind the snapshot boundary.
        leader.takeSnapshot();
        assertEquals(0, leader.getLogSize(),
                "leader log must be empty — lagging follower is behind the snapshot boundary");

        // Heal: lagging follower reconnects, leader sends InstallSnapshot.
        heal(laggingFollower, leader);
        heal(laggingFollower, otherFollower);

        Thread.sleep(1_000); // allow InstallSnapshot + application

        assertEquals(Optional.of("v1"), laggingFollower.read("k1"),
                "k1 must be present — committed before the partition");
        assertEquals(Optional.of("v2"), laggingFollower.read("k2"),
                "k2 must arrive via InstallSnapshot");
        assertEquals(Optional.of("v3"), laggingFollower.read("k3"),
                "k3 must arrive via InstallSnapshot");
    }

    /**
     * When all nodes independently take a snapshot, the cluster must continue
     * to function: a leader must still be elected and new entries committed
     * and replicated. No node should retain compacted entries.
     *
     * <p>This validates that the snapshot offset is consistent across all nodes
     * when every participant compacts its own log independently.
     */
    @Test
    void allNodes_takeSnapshot_clusterContinues() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        leader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        Thread.sleep(500); // allow all followers to apply

        // All nodes compact independently.
        nodes.forEach(RaftNode::takeSnapshot);

        for (RaftNode node : nodes) {
            assertEquals(0, node.getLogSize(),
                    node.getNodeId() + ": log must be empty after snapshot");
        }

        // Cluster must still commit new writes after snapshotting.
        leader.write("put:k3:v3").get(2, TimeUnit.SECONDS);
        Thread.sleep(500);

        for (RaftNode node : nodes) {
            assertEquals(Optional.of("v3"), node.read("k3"),
                    node.getNodeId() + ": k3 must be replicated after post-snapshot write");
        }

        long leaderCount = nodes.stream()
                .filter(n -> n.getState() == RaftState.LEADER)
                .count();
        assertEquals(1, leaderCount, "cluster must have exactly one leader after snapshotting");
    }
}
