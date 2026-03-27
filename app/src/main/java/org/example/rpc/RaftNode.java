package org.example.rpc;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

@Log4j2
public class RaftNode {

    private static final String APPEND_ENTRIES = "appendEntries";
    private static final String REQUEST_VOTE = "requestVote";
    public static final String INSTALL_SNAPSHOT = "installSnapshot";

    @Getter
    private final String nodeId;
    @Getter
    private final int port;
    private RpcServer server;
    private final List<String> peerAddresses;
    @Getter
    private ZNodeTree zNodeTree;
    private List<RpcClient> clients;
    private final Set<String> droppedPeers;

    private final ReentrantReadWriteLock lock;
    private final ScheduledExecutorService electionScheduler;
    private ScheduledFuture<?> electionFuture;
    private ScheduledFuture<?> heartbeatFuture;

    // statelog.get
    private int currentTerm;
    private RaftState state;
    private String votedFor;

    // log & commit state
    private final List<JournalEntry> rLog;       // 1-indexed: entry at i is log.get(i-1)
    private int commitIndex = 0;
    private int lastApplied = 0;

    // state-machine
    private final Map<String, String> stateMachine;

    //
    // leader-only: peer-2-peer replication tracking (key=peerAddress.get(i))
    //
    //  for each server, index of the next log entry
    //  to send to that server (initialized to leader
    //  last log index + 1)
    private final Map<String, Integer> nextIndex;
    //  for each server, index of highest log entry
    //  known to be replicated on server
    //  (initialized to 0, increases monotonically)
    private final Map<String, Integer> matchIndex;

    // pending writes where key = log index
    private final Map<Integer, CompletableFuture<Void>> pendingWrites;

    ///
    ///  snapshot
    ///
    private int snapshotLastIndex = 0;
    private int snapshotLastTerm = 0;

    public RaftNode(String nodeId, int port, List<String> peerAddresses) {
        this.nodeId = nodeId;
        this.port = port;
        this.peerAddresses = peerAddresses;
        this.state = RaftState.FOLLOWER;
        this.currentTerm = 0;
        this.votedFor = null;
        lock = new ReentrantReadWriteLock();
        clients = new ArrayList<>();
        electionScheduler = Executors.newScheduledThreadPool(1);
        droppedPeers = ConcurrentHashMap.newKeySet();
        pendingWrites = new ConcurrentHashMap<>();
        matchIndex = new HashMap<>();
        nextIndex = new HashMap<>();
        stateMachine = new ConcurrentHashMap<>();
        rLog = new ArrayList<>();
    }

    public RaftNode(String nodeId, int port, List<String> peerAddresses, ZNodeTree zNodeTree) {
        this(nodeId, port, peerAddresses);
        this.zNodeTree = zNodeTree;
    }

    private Function<List<String>, String> handleSnapshot() {
        return args -> {
            lock.writeLock().lock();
            try {
                InstallSnapshotRequest req = InstallSnapshotRequest.decode(args);
                log.info(req);
                if (req.term() < currentTerm) {
                    return new InstallSnapshotResponse(currentTerm).toString();
                }
                stepDownIfTermIsHigher(req.term());
                resetElectionTimer();
                if (req.lastIncludedIndex() <= commitIndex) {
                    // stale snapshot — we already have this data
                    return new InstallSnapshotResponse(currentTerm).toString();
                }
                // apply snapshot: replace state machine, update offsets, truncate log
                stateMachine.clear();
                for (String entry : req.data()) {
                    String[] kv = entry.split(":", 2);
                    stateMachine.put(kv[0], kv[1]);
                }
                int entriesToDrop = req.lastIncludedIndex() - snapshotLastIndex;
                if (entriesToDrop <= rLog.size()) {
                    rLog.subList(0, entriesToDrop).clear();
                } else {
                    rLog.clear();
                }
                snapshotLastIndex = req.lastIncludedIndex();
                snapshotLastTerm = req.lastIncludedTerm();
                commitIndex = snapshotLastIndex;
                lastApplied = snapshotLastIndex;
                return new InstallSnapshotResponse(currentTerm).toString();
            } finally {
                lock.writeLock().unlock();
            }
        };
    }

    public void start() {
        server = new RpcServer(port);
        clients = peerAddresses.stream().map(s -> {
            String[] tokens = s.split(":", 2);
            String host = tokens[0];
            int port = Integer.parseInt(tokens[1]);
            return new RpcClient(host, port);
        }).toList();
        server.register(REQUEST_VOTE, handleRequestVote());
        server.register(APPEND_ENTRIES, handleAppendEntries());
        server.register(INSTALL_SNAPSHOT, handleSnapshot());
        becomeFollower(currentTerm);
        resetElectionTimer();
        server.start();
        log.info("RaftNode {} started on port: {}", getNodeId(), port);
    }


    private @NonNull Function<List<String>, String> handleAppendEntries() {
        return args -> {
            lock.writeLock().lock();
            try {
                AppendEntryRequest req = AppendEntryRequest.decode(args);
                log.info(req);

                if (req.term() < currentTerm) {
                    return new AppendEntryResponse(currentTerm, false, 0).encode();
                }
                stepDownIfTermIsHigher(req.term());
                resetElectionTimer();

                // consistency check
                if (req.prevLogIndex() > 0) {
                    boolean mismatch;
                    if (req.prevLogIndex() < snapshotLastIndex) {
                        mismatch = false;   // already covered by snapshot
                    } else if (req.prevLogIndex() == snapshotLastIndex) {
                        mismatch = req.prevLogTerm() != snapshotLastTerm;
                    } else {
                        mismatch = getLastLogIndex() < req.prevLogIndex()
                                || getEntry(req.prevLogIndex()).term() != req.prevLogTerm();
                    }
                    if (mismatch) {
                        return new AppendEntryResponse(currentTerm, false, 0).encode();
                    }
                }

                // append/ overwrite entries
                for (String es : req.entries()) {
                    JournalEntry e = JournalEntry.decode(es);
                    if (getLastLogIndex() >= e.index()) {
                        if (getEntry(e.index()).term() != e.term()) {
                            //conflict: truncate from here onwards
                            while (getLastLogIndex() >= e.index()) {
                                rLog.removeLast();
                            }
                            rLog.add(e);
                        }
                        // else - same entry is already present, so skip
                    } else {
                        rLog.add(e);
                    }
                }

                // advance commit index+apply
                int newCommit = Math.min(req.leaderCommit(), getLastLogIndex());
                if (newCommit > commitIndex) {
                    applyEntries(newCommit);
                }
                return new AppendEntryResponse(currentTerm, true, getLastLogIndex()).encode();
            } finally {
                lock.writeLock().unlock();
            }
        };
    }

    private @NonNull Function<List<String>, String> handleRequestVote() {
        return args -> {
            boolean voteGranted = false;
            lock.writeLock().lock();
            try {
                RequestVoteIn in = RequestVoteIn.decode(args);
                log.info(in);

                int term = in.term();
                String candidateId = in.candidateId();
                int candidateLastLogIndex = in.lastLogIndex();
                int candidateLastLogTerm = in.lastLogTerm();

                stepDownIfTermIsHigher(term);

                boolean logOk = (candidateLastLogTerm > getMyLastLogTerm())
                        || ((candidateLastLogTerm == getMyLastLogTerm()) && (candidateLastLogIndex >= getLastLogIndex()));
                if ((term >= currentTerm) && ((votedFor == null) || candidateId.equals(votedFor)) && logOk) {
                    votedFor = candidateId;
                    voteGranted = true;
                    resetElectionTimer();
                }
            } finally {
                lock.writeLock().unlock();
            }
            RequestVoteOut requestVoteOut = new RequestVoteOut(currentTerm, voteGranted);
            return requestVoteOut.encode();
        };
    }

    private int getMyLastLogTerm() {
        return rLog.isEmpty() ? snapshotLastTerm : getEntry(getLastLogIndex()).term();
    }

    private void becomeLeader() {
        lock.writeLock().lock();
        try {
            if (!state.equals(RaftState.CANDIDATE)) {
                return;
            }
            state = RaftState.LEADER;
            // initialize peer-peer tracking
            int ni = getLastLogIndex() + 1;
            for (String peer : peerAddresses) {
                nextIndex.put(peer, ni);
                matchIndex.put(peer, 0);
            }
        } finally {
            lock.writeLock().unlock();
        }
        // start sending heartbeat to all peers
        Runnable heartbeatTask = getHeartbeatTask();
        heartbeatFuture = electionScheduler.scheduleAtFixedRate(heartbeatTask,
                0,
                100,
                TimeUnit.MILLISECONDS);
    }

    private Runnable getHeartbeatTask() {
        return () -> {
            try {
                // 1. Snapshot everything you need — holds lock only for in-memory reads
                record PeerTask(RpcClient client, String address, String reqType, RaftRequest req) {
                }
                List<PeerTask> batch = new ArrayList<>();

                lock.writeLock().lock();
                try {
                    for (int i = 0; i < clients.size(); i++) {
                        String peerAddress = peerAddresses.get(i);
                        if (droppedPeers.contains(peerAddress)) continue;
                        int ni = nextIndex.getOrDefault(peerAddress, getLastLogIndex() + 1);
                        RpcClient client = clients.get(i);
                        if (ni <= snapshotLastIndex) {
                            batch.add(new PeerTask(client,
                                    peerAddress,
                                    INSTALL_SNAPSHOT,
                                    new InstallSnapshotRequest(getCurrentTerm(), nodeId, snapshotLastIndex, snapshotLastTerm, encode(stateMachine))));
                        } else {
                            int prevIndex = ni - 1;
                            int prevTerm = getPrevTerm(prevIndex);
                            List<String> entries = rLog.subList(ni - snapshotLastIndex - 1, rLog.size())
                                    .stream().map(JournalEntry::encode).toList();
                            batch.add(new PeerTask(
                                    client,
                                    peerAddress,
                                    APPEND_ENTRIES,
                                    new AppendEntryRequest(getCurrentTerm(), getNodeId(), prevIndex, prevTerm, commitIndex, entries)
                            ));
                        }
                    }
                } finally {
                    lock.writeLock().unlock();  // ← released BEFORE any I/O
                }

                // 2. Dispatch all calls without holding the lock
                for (PeerTask t : batch) {
                    try {
                        t.client().call(t.reqType(), t.req().encode())
                                .thenAccept(response -> {
                                    switch (t.reqType()) {
                                        case INSTALL_SNAPSHOT -> {
                                            InstallSnapshotResponse result = InstallSnapshotResponse.decode(response.result());
                                            if (result.term() > currentTerm) {
                                                stepDownIfTermIsHigher(result.term());
                                                return;
                                            }
                                            // follower is now at snapshotLastIndex
                                            lock.readLock().lock();
                                            int sli;
                                            try {
                                                sli = snapshotLastIndex;
                                            } finally {
                                                lock.readLock().unlock();
                                            }
                                            handleSuccessfulReplication(t.address(), sli);
                                        }
                                        case APPEND_ENTRIES -> {
                                            AppendEntryResponse result = new AppendEntryResponse(response.result());
                                            if (result.term() > currentTerm) {
                                                stepDownIfTermIsHigher(result.term());
                                                return;
                                            }
                                            if (result.success()) {
                                                handleSuccessfulReplication(t.address(), result.matchIndex());
                                            } else {
                                                lock.writeLock().lock();
                                                try {
                                                    nextIndex.merge(t.address(), -1, (curr, dec) -> Math.max(1, curr + dec));
                                                } finally {
                                                    lock.writeLock().unlock();
                                                }
                                            }
                                        }
                                        default -> throw new IllegalStateException("Unexpected value: " + t.reqType());
                                    }
                                });
                    } catch (Exception e) {
                        log.error("failed to send {} to peer: {}: error = {}", t.req(), t.address(), e.getMessage());
                    }
                }
            } catch (Exception e) {
                log.error("heartbeat task failed — this will cause elections", e);
            }
        };
    }

    private int getPrevTerm(int prevIndex) {
        int prevTerm;
        if (prevIndex == 0) {
            prevTerm = 0;
        } else if (prevIndex == snapshotLastIndex) {
            prevTerm = snapshotLastTerm;   // boundary: term lives in snapshot, not rLog
        } else {
            prevTerm = getEntry(prevIndex).term();
        }
        return prevTerm;
    }

    private List<String> encode(Map<String, String> stateMachine) {
        lock.readLock().lock();
        try {
            List<String> list = new ArrayList<>();
            stateMachine.forEach((k, v)
                    -> list.add(String.format("%s:%s", k, v)));
            return list;
        } finally {
            lock.readLock().unlock();
        }
    }


    private void handleSuccessfulReplication(String peerAddress, int peerMatchIndex) {
        lock.writeLock().lock();
        try {
            matchIndex.put(peerAddress, peerMatchIndex);
            nextIndex.put(peerAddress, peerMatchIndex + 1);

            // find the highest commit index N > currentIndex, where
            // log[N-1].term = currentTerm
            for (int n = getLastLogIndex(); n > commitIndex; n--) {
                if (getEntry(n).term() != currentTerm) {
                    continue;
                }
                int count = 1;//self
                for (int mi : matchIndex.values()) {
                    if (mi >= n) {
                        count++;
                    }
                }
                if (count > (clients.size() + 1) / 2) {
                    // majority of peers have consensus, so apply
                    applyEntries(n);
                    break;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // apply to state-machine, complete future
    private void applyEntries(int newCommitIndex) {
        commitIndex = newCommitIndex;
        while (lastApplied < commitIndex) {
            lastApplied++;
            JournalEntry e = getEntry(lastApplied);
            if (null == zNodeTree) {
                String[] t = e.command().split(":", 3);
                switch (t[0]) {
                    case "put" -> stateMachine.put(t[1], t[2]);
                    case "delete" -> stateMachine.remove(t[1]);
                    default -> {
                        //no-op: do nothing
                    }
                }
            } else {
                try {
                    zNodeTree.apply(e.command());
                } catch (ZooKeeperException ex) {
                    log.error("failed to apply entries", ex);
                }
            }
            CompletableFuture<?> future = pendingWrites.remove(lastApplied);
            if (future != null) {
                future.complete(null);
            }
        }
    }

    private void becomeFollower(int term) {
        lock.writeLock().lock();
        try {
            state = RaftState.FOLLOWER;
            currentTerm = term;
            votedFor = null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void startElection() {
        record PeerTask(RequestVoteIn voteIn, int votesNeeded) {
        }

        lock.writeLock().lock();
        PeerTask peerTask;
        try {
            if (RaftState.LEADER.equals(state)) {
                return;
            }

            log.info("{} triggered RaftNode.startElection", nodeId);

            // become candidate
            state = RaftState.CANDIDATE;
            currentTerm += 1;
            votedFor = nodeId;

            if (clients.isEmpty()) {
                //  single node - become leader
                becomeLeader();
            } else if (!RaftState.LEADER.equals(getState())) {
                // retry later
                resetElectionTimer();
            }
            RequestVoteIn requestVoteIn = new RequestVoteIn(currentTerm, nodeId, getLastLogIndex(), getMyLastLogTerm());
            peerTask = new PeerTask(requestVoteIn, getVotesNeeded());
        } finally {
            lock.writeLock().unlock();
        }

        List<String> args = peerTask.voteIn().encode();
        AtomicInteger votesReceived = new AtomicInteger(0);
        // send request to all peers
        for (int i = 0; i < clients.size(); i++) {
            RpcClient client = clients.get(i);
            String peerAddress = peerAddresses.get(i);
            if (droppedPeers.contains(peerAddress)) {
                continue;
            }
            try {
                client.call(REQUEST_VOTE, args)
                        .thenAccept(response -> {
                            String result = response.result();
                            RequestVoteOut out = RequestVoteOut.decode(result);
                            if (out.support() && out.term() >= getCurrentTerm()) {
                                if (votesReceived.incrementAndGet() >= peerTask.votesNeeded()) {
                                    becomeLeader();
                                }
                            }
                        });
            } catch (Exception e) {
                log.error("[REQUEST_VOTE]: {} is unable to reach [{}}:{}}]: {}}",
                        nodeId,
                        client.getHost(),
                        client.getPort(),
                        e.getMessage());
            }
        }

    }

    private int getVotesNeeded() {
        int nodeInCluster = clients.size() + 1;
        int majority = (nodeInCluster / 2) + 1; // e.g. 3-node: (2+1)/2+1 = 2
        // -1 because we already count our own vote
        return majority - 1;
    }

    private void resetElectionTimer() {
        if (null != electionFuture) {
            electionFuture.cancel(false);
        }
        //  schedule next election
        long delay = 300 + ThreadLocalRandom.current().nextInt(300);
        electionFuture = electionScheduler.schedule(this::startElection, delay, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (clients != null) {
            for (RpcClient client : clients) {
                if (client != null) {
                    try {
                        client.close();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
        electionScheduler.shutdownNow();
        if (server != null) {
            try {
                server.close();
            } catch (Exception ignored) {
            }
        }
        log.info("{} stopped", nodeId);
    }

    public RaftState getState() {
        lock.readLock().lock();
        try {
            return state;
        } finally {
            lock.readLock().unlock();
        }
    }

    public boolean isLeader() {
        return getState() == RaftState.LEADER;
    }

    public int getCurrentTerm() {
        lock.readLock().lock();
        try {
            return currentTerm;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void stepDownIfTermIsHigher(int term) {
        lock.writeLock().lock();
        try {
            if (term > currentTerm) {
                currentTerm = term;
                state = RaftState.FOLLOWER;
                votedFor = null; // reset vote
                // cancel sending heartbeats as this node is not leader anymore
                if (heartbeatFuture != null) {
                    heartbeatFuture.cancel(false);
                }
                // fail pending writes as this node is not leader anymore
                pendingWrites.values()
                        .forEach(f
                                -> f.completeExceptionally(new IllegalStateException("leader stepped down")));
                pendingWrites.clear();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public CompletableFuture<Void> write(String command) {
        lock.writeLock().lock();
        try {
            if (!RaftState.LEADER.equals(state)) {
                throw new IllegalStateException("RaftNode is not leader");
            }
            int index = getLastLogIndex() + 1;
            JournalEntry entry = new JournalEntry(index, currentTerm, command);
            rLog.add(entry);
            CompletableFuture<Void> future = new CompletableFuture<>();
            pendingWrites.put(index, future);
            return future; // Complete later when entry is committed + applied
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Optional<String> read(String key) {
        return Optional.ofNullable(stateMachine.get(key));
    }

    public int getCommitIndex() {
        lock.readLock().lock();
        try {
            return commitIndex;
        } finally {
            lock.readLock().unlock();
        }
    }

    List<JournalEntry> getrLog() {
        lock.readLock().lock();
        try {
            return List.copyOf(rLog);
        } finally {
            lock.readLock().unlock();
        }
    }

    public void dropMessagesTo(String peerAddress) {
        droppedPeers.add(peerAddress);
    }

    public void restoreMessagesTo(String peerAddress) {
        droppedPeers.remove(peerAddress);
    }

    public void takeSnapshot() {
        lock.writeLock().lock();
        try {
            int entriesToRemove = commitIndex - snapshotLastIndex;
            int newSnapshotLastTerm = rLog.get(entriesToRemove - 1).term(); // capture before clearing
            rLog.subList(0, entriesToRemove).clear();
            snapshotLastIndex = commitIndex;
            snapshotLastTerm = newSnapshotLastTerm;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public int getLogSize() {
        lock.readLock().lock();
        try {
            return rLog.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private JournalEntry getEntry(int index) {
        lock.readLock().lock();
        try {
            return rLog.get(index - snapshotLastIndex - 1);
        } finally {
            lock.readLock().unlock();
        }
    }

    private int getLastLogIndex() {
        lock.readLock().lock();
        try {
            return snapshotLastIndex + rLog.size();
        } finally {
            lock.readLock().unlock();
        }
    }
}
