package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3, Step 3 — Network Partition Handling
 *
 * A network partition splits the cluster into two groups that cannot communicate.
 * Raft's quorum requirement ensures only the majority side can make progress.
 * This step exercises the full partition/heal cycle.
 *
 * <h3>Key invariants</h3>
 * <ul>
 *   <li>A minority partition (fewer than quorum nodes) can never commit new entries.
 *   <li>A majority partition (quorum or more nodes) elects a new leader and continues.
 *   <li>A stale leader isolated from the cluster eventually steps down when the
 *       partition heals, because the new leader's {@code AppendEntries} carries a
 *       higher term.
 *   <li>A node that rejoins after a partition catches up via the normal backfill
 *       mechanism (decreasing {@code nextIndex} until the leader finds the common
 *       prefix).
 * </ul>
 *
 * <h3>New APIs required on RaftNode</h3>
 * <pre>
 *   void dropMessagesTo(String peerAddress)
 *       — Simulates a one-directional network failure: this node will not send
 *         any RPCs to {@code peerAddress}. Outgoing calls to that address are
 *         silently skipped (as if the network dropped the packets).
 *         Call on BOTH endpoints to simulate a full bidirectional partition.
 *
 *   void restoreMessagesTo(String peerAddress)
 *       — Lifts the simulated failure: RPCs to {@code peerAddress} resume normally.
 *
 *   int getPort()
 *       — Returns this node's listening port (needed by test helpers to build
 *         peer addresses for partition/heal calls).
 * </pre>
 *
 * <h3>Implementation notes</h3>
 * <ul>
 *   <li>Maintain a {@code Set<String> droppedPeers} (use
 *       {@code ConcurrentHashMap.newKeySet()} for thread safety).
 *   <li>In {@code getHeartbeatTask()} skip any peer whose address is in
 *       {@code droppedPeers} — do not even attempt the RPC call.
 *   <li>In {@code startElection()} skip partitioned peers the same way.
 *   <li>No changes needed to incoming handlers ({@code handleAppendEntries},
 *       {@code handleRequestVote}) — those paths are already correct.
 *       Blocking outgoing traffic is sufficient because:
 *       (a) the isolated node cannot replicate → cannot commit,
 *       (b) after healing, the new leader's incoming {@code AppendEntries}
 *           with a higher term triggers {@code stepDownIfTermIsHigher}.
 * </ul>
 *
 * <h3>Deferred fix required for full correctness</h3>
 * <p>The heartbeat {@code thenAccept} callback currently does not call
 * {@code stepDownIfTermIsHigher} when a peer's response carries a higher term.
 * Add that check here (it was deferred from Step 2):
 * <pre>
 *   if (result.term() > currentTerm) {
 *       stepDownIfTermIsHigher(result.term());
 *   }
 * </pre>
 * This ensures a stale leader steps down as soon as it receives a response from
 * any node that has already advanced to a higher term — not just when it receives
 * an incoming {@code AppendEntries}. Both code paths are needed for correctness.
 */
@Timeout(30)
class RaftNetworkPartitionTest {

    // Ports 7201–7203: reserved for partition tests.
    static final int BASE_PORT = 7201;

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

    /**
     * Simulates a full bidirectional partition between {@code a} and {@code b}.
     * Neither node can send RPCs to the other.
     */
    private void partition(RaftNode a, RaftNode b) {
        a.dropMessagesTo("localhost:" + b.getPort());
        b.dropMessagesTo("localhost:" + a.getPort());
    }

    /**
     * Heals the bidirectional partition between {@code a} and {@code b}.
     */
    private void heal(RaftNode a, RaftNode b) {
        a.restoreMessagesTo("localhost:" + b.getPort());
        b.restoreMessagesTo("localhost:" + a.getPort());
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /**
     * When the leader is isolated from both followers it no longer has a quorum
     * and must not commit any new entries.
     *
     * <p>The isolated leader only has one vote (itself), which is less than the
     * majority of 2 required in a 3-node cluster. Any {@code write()} issued
     * against it will therefore never complete.
     *
     * <p>Note: entries committed <em>before</em> the partition are safe — the
     * followers already have them and the state machine is unaffected.
     */
    @Test
    void isolatedLeader_cannotCommitNewWrite() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);
        List<RaftNode> followers = nodes.stream()
                .filter(n -> n != leader)
                .toList();

        // Commit one entry before the partition — it must remain safe.
        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // allow followers to apply

        // Isolate the leader: partition from both followers, both directions.
        RaftNode f1 = followers.get(0);
        RaftNode f2 = followers.get(1);
        partition(leader, f1);
        partition(leader, f2);

        // A write issued to the isolated leader must not commit.
        CompletableFuture<Void> hangingWrite = leader.write("put:k2:v2");
        Thread.sleep(500); // well past one heartbeat round

        assertFalse(hangingWrite.isDone(),
                "write to isolated leader must not commit — no quorum");

        // Pre-partition data is still readable on the isolated leader.
        assertEquals(Optional.of("v1"), leader.read("k1"),
                "entry committed before partition must still be present");
    }

    /**
     * When the leader is isolated, the two remaining followers form a majority
     * and elect a new leader. The new leader must be able to commit fresh writes.
     *
     * <p>The survivors have quorum (2 of 3 nodes). They will time out on the
     * leader's heartbeat, trigger an election, and elect one of themselves.
     */
    @Test
    void majorityPartition_electsNewLeader_andContinuesServing() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode originalLeader = waitForLeader(nodes, 5_000);
        List<RaftNode> followers = nodes.stream()
                .filter(n -> n != originalLeader)
                .toList();
        RaftNode f1 = followers.get(0);
        RaftNode f2 = followers.get(1);

        // Commit k1 before the split so both followers already have it.
        originalLeader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        Thread.sleep(300);

        // Fully isolate the original leader.
        partition(originalLeader, f1);
        partition(originalLeader, f2);

        // The two survivors must elect a new leader.
        RaftNode newLeader = waitForLeader(List.of(f1, f2), 5_000);

        // New leader must be in a strictly higher term than the original.
        assertTrue(newLeader.getCurrentTerm() > originalLeader.getCurrentTerm(),
                "new leader must have advanced to a higher term");

        // New leader can commit fresh writes despite the partition.
        newLeader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        assertEquals(Optional.of("v2"), newLeader.read("k2"),
                "new leader must accept and commit writes after election");

        // Entry committed before the split is still visible.
        assertEquals(Optional.of("v1"), newLeader.read("k1"),
                "entry committed before partition must survive on new leader");
    }

    /**
     * When the partition heals, the stale leader receives an {@code AppendEntries}
     * from the new leader carrying a higher term. This triggers
     * {@code stepDownIfTermIsHigher}, and the stale leader transitions to FOLLOWER.
     * The cluster must converge to exactly one leader.
     *
     * <p>This test also validates the deferred fix from Step 2: the heartbeat
     * {@code thenAccept} must call {@code stepDownIfTermIsHigher} when it sees a
     * response term higher than {@code currentTerm}. Both code paths (incoming
     * AppendEntries and outgoing heartbeat response) contribute to fast step-down.
     */
    @Test
    void isolatedLeader_stepsDown_whenPartitionHeals() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode originalLeader = waitForLeader(nodes, 5_000);
        List<RaftNode> followers = nodes.stream()
                .filter(n -> n != originalLeader)
                .toList();
        RaftNode f1 = followers.get(0);
        RaftNode f2 = followers.get(1);

        // Isolate original leader from the cluster.
        partition(originalLeader, f1);
        partition(originalLeader, f2);

        // Let survivors elect a new leader (higher term).
        waitForLeader(List.of(f1, f2), 5_000);

        // Heal partition — original leader can now communicate with cluster again.
        heal(originalLeader, f1);
        heal(originalLeader, f2);

        // Give the cluster time to stabilise: new leader sends heartbeats to
        // original leader → originalLeader sees higher term → steps down.
        waitForLeader(nodes, 5_000);

        assertNotEquals(RaftState.LEADER, originalLeader.getState(),
                "original leader must step down after partition heals — " +
                "new leader's higher-term AppendEntries triggers stepDownIfTermIsHigher");

        long leaderCount = nodes.stream()
                .filter(n -> n.getState() == RaftState.LEADER)
                .count();
        assertEquals(1, leaderCount, "cluster must converge to exactly one leader");
    }

    /**
     * A follower that was isolated during some writes re-joins and catches up
     * via the leader's normal backfill mechanism: the leader decrements
     * {@code nextIndex} until it finds the common prefix, then replays all
     * missing entries in one or more rounds.
     *
     * <p>This test differs from {@code laggingFollower_catchesUp_afterRejoin}
     * in {@code RaftLogReplicationTest}: here the node is <em>partitioned</em>
     * (alive but unreachable) rather than fully stopped and replaced. The
     * reconnection path is therefore via {@code restoreMessagesTo} rather than
     * restarting the server.
     */
    @Test
    void partitionedFollower_catchesUp_afterHeal() throws Exception {
        cluster(3).forEach(RaftNode::start);
        RaftNode leader = waitForLeader(nodes, 5_000);
        RaftNode partitionedFollower = nodes.stream()
                .filter(n -> n != leader)
                .findFirst().orElseThrow();
        RaftNode otherFollower = nodes.stream()
                .filter(n -> n != leader && n != partitionedFollower)
                .findFirst().orElseThrow();

        // Commit k1 while all nodes are healthy.
        leader.write("put:k1:v1").get(2, TimeUnit.SECONDS);
        Thread.sleep(300); // allow follower application

        // Partition one follower from both leader and the other follower.
        partition(partitionedFollower, leader);
        partition(partitionedFollower, otherFollower);

        // Commit k2 and k3 — only leader + otherFollower form the quorum.
        leader.write("put:k2:v2").get(2, TimeUnit.SECONDS);
        leader.write("put:k3:v3").get(2, TimeUnit.SECONDS);

        // Heal the partition — partitionedFollower can talk to leader again.
        heal(partitionedFollower, leader);
        heal(partitionedFollower, otherFollower);

        // Allow backfill to propagate (a few heartbeat intervals).
        Thread.sleep(1_000);

        assertEquals(Optional.of("v1"), partitionedFollower.read("k1"),
                "k1 must be present — it was committed before the partition");
        assertEquals(Optional.of("v2"), partitionedFollower.read("k2"),
                "k2 must be backfilled after partition heals");
        assertEquals(Optional.of("v3"), partitionedFollower.read("k3"),
                "k3 must be backfilled after partition heals");

        // Sanity: the cluster must still have exactly one leader.
        long leaderCount = nodes.stream()
                .filter(n -> n.getState() == RaftState.LEADER)
                .count();
        assertEquals(1, leaderCount,
                "cluster must still have exactly one leader after partition heals");
    }
}
