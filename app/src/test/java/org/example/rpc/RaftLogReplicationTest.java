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
 * Phase 3, Step 2 — Raft Log Replication
 *
 * Once a leader is elected, it must safely replicate client writes to the rest
 * of the cluster before acknowledging them. This step builds the append-only log,
 * the full AppendEntries replication protocol, the commit rule, and state-machine apply.
 *
 * <h3>Key invariants</h3>
 * <ul>
 *   <li>A write is only acknowledged after it is committed (majority acknowledgement).
 *   <li>A committed entry is never lost — it survives any future leader crash.
 *   <li>Followers apply committed entries to their own state machine via {@code leaderCommit}.
 *   <li>Only the current leader accepts writes; followers must reject them immediately.
 * </ul>
 *
 * <h3>New APIs required on RaftNode</h3>
 * <pre>
 *   CompletableFuture&lt;Void&gt; write(String command)
 *       — appends entry to log, replicates, completes when committed AND applied.
 *       — throws IllegalStateException if this node is not the leader.
 *
 *   Optional&lt;String&gt; read(String key)
 *       — reads from the applied state machine (not the uncommitted log).
 *
 *   int getCommitIndex()
 *       — highest log index known to be committed.
 *
 *   List&lt;JournalEntry&gt; getLog()
 *       — snapshot of the full log for test inspection.
 * </pre>
 *
 * <h3>JournalEntry changes</h3>
 * Update the record to: {@code JournalEntry(int index, int term, String command)}
 *
 * <h3>Protocol changes</h3>
 * <ul>
 *   <li>AppendEntries gains: {@code prevLogIndex}, {@code prevLogTerm},
 *       {@code leaderCommit}, and the list of new entries to replicate.
 *   <li>RequestVote gains: {@code lastLogIndex}, {@code lastLogTerm} so a voter
 *       can reject a candidate whose log is less up-to-date (prevents electing a
 *       node that is missing committed entries).
 * </ul>
 *
 * <h3>State machine</h3>
 * Commands use the format {@code "put:key:value"} or {@code "delete:key"}.
 * Committed entries are applied in order to an in-memory {@code Map<String, String>}.
 */
@Timeout(30)
class RaftLogReplicationTest {

    // Ports 7101–7103: reserved for log-replication tests.
    static final int BASE_PORT = 7101;

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

    // ── tests ─────────────────────────────────────────────────────────────────

    /**
     * write() must append the entry to the leader's own log immediately.
     * The JournalEntry must carry the correct 1-based index, the leader's current
     * term, and the original command string.
     */
    @Test
    void write_appendsEntryToLeaderLog() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:city:Tokyo").get(2, TimeUnit.SECONDS);

        List<JournalEntry> log = leader.getrLog();
        assertEquals(1, log.size());

        JournalEntry entry = log.getFirst();
        assertEquals(1, entry.index());
        assertEquals(leader.getCurrentTerm(), entry.term());
        assertEquals("put:city:Tokyo", entry.command());
    }

    /**
     * write() must complete only after the entry is committed on a majority
     * (leader + at least 1 follower in a 3-node cluster).
     * The leader's commitIndex must advance to 1.
     */
    @Test
    void write_completesAfterMajorityAck() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        assertEquals(0, leader.getCommitIndex(), "no entries committed initially");

        leader.write("put:x:1").get(2, TimeUnit.SECONDS);

        assertEquals(1, leader.getCommitIndex(),
                "commitIndex must advance to 1 after majority acknowledgement");
    }

    /**
     * write() completes after the entry is both committed AND applied to the
     * state machine. An immediate read following write().get() must return the
     * written value — no sleep needed.
     */
    @Test
    void committedEntry_isAppliedToLeaderStateMachine() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:city:Tokyo").get(2, TimeUnit.SECONDS);

        assertEquals(Optional.of("Tokyo"), leader.read("city"));
        assertEquals(Optional.empty(), leader.read("missing"),
                "key that was never written must return empty");
    }

    /**
     * Multiple writes to the same key must be applied in log order.
     * The state machine must reflect the final committed value and commitIndex
     * must equal the total number of writes.
     */
    @Test
    void multipleWrites_appliedInOrder() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:counter:1").get(2, TimeUnit.SECONDS);
        leader.write("put:counter:2").get(2, TimeUnit.SECONDS);
        leader.write("put:counter:3").get(2, TimeUnit.SECONDS);

        assertEquals(3, leader.getrLog().size());
        assertEquals(3, leader.getCommitIndex());
        assertEquals(Optional.of("3"), leader.read("counter"),
                "last write for the same key must win");
    }

    /**
     * Committed entries propagate to follower state machines via the
     * {@code leaderCommit} field in subsequent AppendEntries messages.
     * After one or two heartbeat intervals (≥ 200ms), all nodes must have
     * applied the entry.
     */
    @Test
    void committedEntry_propagatesToFollowerStateMachines() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:city:Tokyo").get(2, TimeUnit.SECONDS);

        // Give followers time to receive the heartbeat carrying leaderCommit = 1
        // and apply the entry to their own state machines.
        // Heartbeat interval = 100ms; 500ms is 5× that, well within margin.
        Thread.sleep(500);

        for (RaftNode node : nodes) {
            assertEquals(Optional.of("Tokyo"), node.read("city"),
                    node.getNodeId() + " should have applied the committed entry");
        }
    }

    /**
     * A write directed at a follower must be rejected immediately.
     * Clients must route writes to the current leader.
     */
    @Test
    void write_throwsOnNonLeader() throws Exception {
        cluster(3).forEach(RaftNode::start);
        waitForLeader(nodes, 5_000);

        RaftNode follower = nodes.stream()
                .filter(n -> n.getState() == RaftState.FOLLOWER)
                .findFirst()
                .orElseThrow();

        assertThrows(IllegalStateException.class, () -> follower.write("put:x:1"),
                "write() on a follower must throw IllegalStateException");
    }

    /**
     * Raft's core durability guarantee: a committed entry is never lost.
     *
     * After the write is committed and propagated to followers (via heartbeats
     * carrying {@code leaderCommit}), crashing the leader leaves both followers
     * with the entry applied. Whichever survivor wins the next election can serve
     * reads for the written key immediately.
     *
     * The sleep before the crash is intentional: it ensures the old leader's
     * heartbeats have carried {@code leaderCommit = 1} to the followers. Without
     * it, a follower might win the election with the entry in its log but not yet
     * applied (its own {@code commitIndex} is still 0). Solving that without a
     * sleep requires a leader to append a no-op entry on election to re-commit
     * all prior entries — an optimisation left for Step 3.
     */
    @Test
    void committedWrite_survivesLeaderCrash() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:key:durable").get(2, TimeUnit.SECONDS);

        // Allow heartbeats to carry leaderCommit to followers so they apply the
        // entry before the leader is killed.
        Thread.sleep(500);

        leader.stop();
        List<RaftNode> survivors = nodes.stream()
                .filter(n -> n != leader)
                .toList();

        RaftNode newLeader = waitForLeader(survivors, 5_000);

        assertEquals(Optional.of("durable"), newLeader.read("key"),
                "a committed and applied entry must survive a leader crash");
    }

    // ── tests targeting bugs identified in code review ────────────────────────

    /**
     * The {@code delete:key} command must remove the key from the applied state
     * machine. A subsequent read must return {@code Optional.empty()}.
     *
     * Also verifies that a {@code put} after a {@code delete} on the same key
     * results in the new value, confirming correct log ordering.
     */
    @Test
    void delete_removesKeyFromStateMachine() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        leader.write("put:city:Tokyo").get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of("Tokyo"), leader.read("city"));

        leader.write("delete:city").get(2, TimeUnit.SECONDS);
        assertEquals(Optional.empty(), leader.read("city"),
                "key must be absent after delete is committed");

        // put after delete must resurrect the key
        leader.write("put:city:Osaka").get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of("Osaka"), leader.read("city"),
                "put after delete must work — key must be present again");
    }

    /**
     * After the original leader crashes, the successor leader must accept and
     * commit new writes, not just serve reads of previously committed entries.
     *
     * This is distinct from {@code committedWrite_survivesLeaderCrash} which
     * only reads an already-committed value. Here we verify the full write path
     * functions on the successor.
     */
    @Test
    void newLeader_canCommitNewWrites_afterPredecessorCrash() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode originalLeader = waitForLeader(nodes, 5_000);

        originalLeader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // propagate k1 to followers

        originalLeader.stop();
        List<RaftNode> survivors = nodes.stream()
                .filter(n -> n != originalLeader)
                .toList();

        RaftNode newLeader = waitForLeader(survivors, 5_000);

        // New leader must commit brand-new writes
        newLeader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of("v2"), newLeader.read("k2"),
                "new leader must commit fresh writes");
        // Entry committed before the crash must still be visible
        assertEquals(Optional.of("v1"), newLeader.read("k1"),
                "entry committed before crash must still be present");
    }

    /**
     * Bug: {@code nextIndex.merge(peer, -1, Integer::sum)} can underflow to 0 or
     * below if a follower rejects every round. At {@code nextIndex = 0} the
     * leader computes {@code prevLogIndex = -1} and calls
     * {@code log.subList(-1, n)} which throws {@code IndexOutOfBoundsException},
     * silently killing heartbeats for that peer.
     *
     * <p>In practice {@code prevLogIndex = 0} always passes the consistency check
     * (no previous entry to validate), so this only triggers if a node has a
     * conflicting entry at index 1 from a prior leadership term — a Step 3
     * scenario. This test exercises the decrement path with a fresh (empty-log)
     * rejoin to confirm {@code nextIndex} stays ≥ 1 and backfill completes.
     */
    @Test
    void laggingFollower_catchesUp_afterRejoin() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);

        // Commit 2 entries — both followers receive them
        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        leader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // allow followers to receive and apply

        // Isolate one follower — it will miss future entries
        RaftNode staleFollower = nodes.stream()
                .filter(n -> n.getState() == RaftState.FOLLOWER)
                .findFirst().orElseThrow();
        staleFollower.stop();

        // Commit one more entry — still a majority (leader + remaining follower)
        leader.write("put:k3:v3").get(2, TimeUnit.SECONDS);

        // Replace the stopped follower with a fresh node on the same port.
        // The leader's nextIndex for this peer starts at 4 (after 3 committed
        // entries), so the first heartbeat has prevLogIndex=3 — rejected by the
        // empty-log node. The leader decrements: 4 → 3 → 2 → 1, then sends
        // prevLogIndex=0 with all 3 entries, which always passes the
        // consistency check. All 3 entries are applied in one round.
        int idx = Integer.parseInt(staleFollower.getNodeId().split("-")[1]);
        List<String> peers = new ArrayList<>(addresses(3));
        peers.remove(idx);
        RaftNode freshFollower = new RaftNode(
                staleFollower.getNodeId(), BASE_PORT + idx, peers);
        nodes.add(freshFollower);
        freshFollower.start();

        // Give the leader time to backfill: 3 decrement rounds + 1 success round
        // = 4 heartbeat intervals (400ms), plus margin.
        Thread.sleep(1_000);

        assertEquals(Optional.of("v1"), freshFollower.read("k1"),
                "backfill must include k1");
        assertEquals(Optional.of("v2"), freshFollower.read("k2"),
                "backfill must include k2");
        assertEquals(Optional.of("v3"), freshFollower.read("k3"),
                "backfill must include k3 — the entry missed during isolation");
    }

    /**
     * Bug: the heartbeat {@code thenAccept} callback does not call
     * {@code stepDownIfTermIsHigher} when a peer's response carries a term
     * higher than the leader's own. A stale leader would stay LEADER instead of
     * stepping down, creating a split-brain until it received an incoming message
     * with the higher term.
     *
     * <p>This test exercises the cluster-convergence invariant: after stopping the
     * leader and letting a new one be elected, reintroducing a fresh node on the
     * old leader's port must result in exactly one LEADER. The fresh node must
     * eventually step down to FOLLOWER — it must not win a new election — because
     * its empty log is less up-to-date than the surviving nodes' logs.
     *
     * <p>The step-down happens via the incoming AppendEntries from the real leader
     * (which also carries a higher term), not solely via the heartbeat-response
     * check. Full isolation of the heartbeat-response path requires the partition
     * simulation infrastructure introduced in Step 3.
     */
    @Test
    void rejoinedNode_convergesToFollower_neverUsurpsLeadership() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode originalLeader = waitForLeader(nodes, 5_000);

        originalLeader.write("put:x:1").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // propagate to followers

        int originalTerm = originalLeader.getCurrentTerm();

        // Stop the original leader — survivors elect a new leader in a higher term
        originalLeader.stop();
        List<RaftNode> survivors = nodes.stream()
                .filter(n -> n != originalLeader).toList();

        RaftNode newLeader = waitForLeader(survivors, 5_000);
        assertTrue(newLeader.getCurrentTerm() > originalTerm,
                "new leader must be in a higher term");

        // Reintroduce a FRESH node on the original leader's port (empty log, term 0).
        // This node cannot win an election: its log is less up-to-date than the
        // survivors', so logOk=false in handleRequestVote will reject its candidacy.
        int leaderIdx = Integer.parseInt(originalLeader.getNodeId().split("-")[1]);
        List<String> peers = new ArrayList<>(addresses(3));
        peers.remove(leaderIdx);
        RaftNode rejoinedNode = new RaftNode(
                originalLeader.getNodeId(), BASE_PORT + leaderIdx, peers);
        nodes.add(rejoinedNode);
        rejoinedNode.start();

        // Allow cluster to stabilise
        Thread.sleep(700);

        // Exclude the stopped originalLeader — stop() shuts down the scheduler
        // and closes connections but does not reset the state field, so it would
        // still report LEADER. Only count nodes that are actively running.
        long leaderCount = nodes.stream()
                .filter(n -> n != originalLeader)
                .filter(n -> n.getState() == RaftState.LEADER)
                .count();
        assertEquals(1, leaderCount,
                "cluster must have exactly one leader after node rejoins");

        assertNotEquals(RaftState.LEADER, rejoinedNode.getState(),
                "rejoined node with empty log must not win leadership — " +
                "it must step down to FOLLOWER");
    }
}
