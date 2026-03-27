package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3, Step 1 — Raft Leader Election
 *
 * Raft uses randomised election timeouts and majority votes to safely elect
 * exactly one leader per term, without any external coordinator.
 *
 * Key invariants:
 *   - A node votes for at most ONE candidate per term.
 *   - A candidate needs votes from STRICTLY MORE THAN HALF the cluster.
 *   - Any node that sees a higher term immediately reverts to FOLLOWER.
 *   - Only a LEADER sends heartbeats (AppendEntries with no log entries).
 */
@Timeout(30)
class RaftLeaderElectionTest {

    // Ports 7001–7005: reserved for Raft tests.
    static final int BASE_PORT = 7001;

    List<RaftNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(node -> {
            try { node.stop(); } catch (Exception ignored) {}
        });
        nodes.clear();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** Builds addresses ["localhost:7001", "localhost:7002", ...] for n nodes. */
    private List<String> addresses(int n) {
        List<String> addrs = new ArrayList<>();
        for (int i = 0; i < n; i++) addrs.add("localhost:" + (BASE_PORT + i));
        return addrs;
    }

    /** Creates and registers n nodes that all know about each other. */
    private List<RaftNode> cluster(int n) {
        List<String> addrs = addresses(n);
        for (int i = 0; i < n; i++) {
            List<String> peers = new ArrayList<>(addrs);
            peers.remove(i); // a node does not list itself as a peer
            RaftNode node = new RaftNode("node-" + i, BASE_PORT + i, peers);
            nodes.add(node);
        }
        return nodes;
    }

    /**
     * Polls until exactly one LEADER exists among the given nodes, or throws.
     * Returns the leader node.
     */
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
        long leaderCount = candidates.stream()
                .filter(n -> n.getState() == RaftState.LEADER).count();
        throw new AssertionError(
                "Expected exactly 1 leader within " + timeoutMs + "ms, got " + leaderCount);
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /**
     * A single-node cluster is a degenerate case: the node needs only its own
     * vote to win the election (1 out of 1 = majority).
     * It should elect itself as soon as its election timer fires.
     */
    @Test
    void singleNode_electsItself_asSoonAsTimerFires() throws Exception {
        RaftNode solo = new RaftNode("solo", BASE_PORT, List.of());
        nodes.add(solo);
        solo.start();

        RaftNode leader = waitForLeader(nodes, 2_000);
        assertEquals("solo", leader.getNodeId());
        assertEquals(RaftState.LEADER, leader.getState());
        assertTrue(leader.getCurrentTerm() >= 1);
    }

    /**
     * In a 3-node cluster, exactly one node must become LEADER and the other
     * two must remain FOLLOWER. This must happen within a few election timeouts.
     */
    @Test
    void threeNodeCluster_electsExactlyOneLeader() throws Exception {
        cluster(3).forEach(RaftNode::start);

        RaftNode leader = waitForLeader(nodes, 5_000);
        long followerCount = nodes.stream()
                .filter(n -> n.getState() == RaftState.FOLLOWER).count();

        assertEquals(RaftState.LEADER, leader.getState());
        assertEquals(2, followerCount, "remaining 2 nodes must be followers");

        // All nodes must agree on the same term
        int leaderTerm = leader.getCurrentTerm();
        nodes.forEach(n -> assertEquals(leaderTerm, n.getCurrentTerm(),
                "all nodes must be in the same term after election"));
    }

    /**
     * Once a leader is elected it sends periodic heartbeats (AppendEntries with
     * no log entries). Followers that receive heartbeats must NOT start a new
     * election. The term must not advance.
     */
    @Test
    void leader_sendsHeartbeats_suppressingNewElections() throws Exception {
        cluster(3).forEach(RaftNode::start);

        RaftNode leader = waitForLeader(nodes, 5_000);
        int termAfterFirstElection = leader.getCurrentTerm();

        // Sleep for 3× the max election timeout — would cause re-election
        // if heartbeats were not being sent.
        Thread.sleep(1_800);

        assertEquals(RaftState.LEADER, leader.getState(),
                "leader must still be leader after sleeping");
        assertEquals(termAfterFirstElection, leader.getCurrentTerm(),
                "term must not advance when leader is alive");
    }

    /**
     * When the leader stops (simulating a crash), the remaining two nodes must
     * detect the absence of heartbeats and elect a new leader among themselves.
     * The new term must be higher than the old term.
     */
    @Test
    void leaderCrash_triggersFreshElection_withHigherTerm() throws Exception {
        cluster(3).forEach(RaftNode::start);

        RaftNode oldLeader = waitForLeader(nodes, 5_000);
        int termBeforeCrash = oldLeader.getCurrentTerm();

        // Crash the leader
        oldLeader.stop();
        List<RaftNode> survivors = nodes.stream()
                .filter(n -> n != oldLeader).toList();

        RaftNode newLeader = waitForLeader(survivors, 5_000);
        assertNotEquals(oldLeader.getNodeId(), newLeader.getNodeId(),
                "old crashed leader must not be re-elected");
        assertTrue(newLeader.getCurrentTerm() > termBeforeCrash,
                "new election must have a higher term than the crashed leader's term");
    }

    /**
     * Raft's most critical safety rule: if any node sees a message with a term
     * higher than its own, it must immediately revert to FOLLOWER.
     *
     * Here we simulate this by starting a 5-node cluster, waiting for a leader,
     * then stopping all nodes and restarting them. The restarted nodes will
     * have lost their in-memory voted-for state; a new election will start, and
     * the eventual term must be higher than the original.
     *
     * A more direct test: a node currently in state LEADER that receives
     * a RequestVote or AppendEntries with term > currentTerm must step down.
     */
    @Test
    void nodeWithHigherTerm_forcesCurrentLeader_toStepDown() throws Exception {
        cluster(3).forEach(RaftNode::start);

        RaftNode leader = waitForLeader(nodes, 5_000);
        int originalTerm = leader.getCurrentTerm();

        // Tell the leader about a higher term directly.
        // This simulates receiving a message from a node that has been in a
        // higher term (e.g., it went through a partition with extra elections).
        leader.stepDownIfTermIsHigher(originalTerm + 5);

        // Leader must immediately step down
        Thread.sleep(100);
        assertNotEquals(RaftState.LEADER, leader.getState(),
                "leader must step down when it learns of a higher term");
        assertEquals(originalTerm + 5, leader.getCurrentTerm(),
                "node must update its term to the higher term it saw");
    }

    /**
     * In a 5-node cluster, the leader can be lost and two more nodes can go
     * down — yet the remaining 3 nodes (a majority of 5) must still be able
     * to elect a new leader.
     *
     * This shows the quorum property: a cluster of N tolerates floor(N/2)
     * failures.
     */
    @Test
    void fiveNodeCluster_toleratesTwoFailures_andElectsNewLeader() throws Exception {
        List<RaftNode> all = new ArrayList<>();
        List<String> addrs = addresses(5);
        for (int i = 0; i < 5; i++) {
            List<String> peers = new ArrayList<>(addrs);
            peers.remove(i);
            RaftNode n = new RaftNode("node-" + i, BASE_PORT + i, peers);
            all.add(n);
            nodes.add(n);
        }
        all.forEach(RaftNode::start);

        RaftNode first = waitForLeader(all, 5_000);

        // Stop the leader and one additional follower (2 failures)
        RaftNode followerToKill = all.stream()
                .filter(n -> n != first)
                .findFirst().orElseThrow();
        first.stop();
        followerToKill.stop();

        List<RaftNode> majority = all.stream()
                .filter(n -> n != first && n != followerToKill).toList();
        assertEquals(3, majority.size());

        // Remaining 3 nodes must elect a new leader
        waitForLeader(majority, 5_000);
    }
}
