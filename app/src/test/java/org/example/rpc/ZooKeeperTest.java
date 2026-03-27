package org.example.rpc;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Phase 3, Step 5 — Mini ZooKeeper
 * <p>
 * Build a distributed coordination service on top of the Raft layer.
 * ZooKeeper provides a hierarchical ZNode namespace, atomic operations,
 * one-shot watches, ephemeral nodes, and sequential nodes.
 *
 * <h3>New enums and records</h3>
 * <pre>
 * enum ZNodeMode { PERSISTENT, EPHEMERAL, PERSISTENT_SEQUENTIAL, EPHEMERAL_SEQUENTIAL }
 *
 * enum WatchEventType { CREATED, DATA_CHANGED, DELETED }
 *
 * record WatchEvent(String path, WatchEventType type) {}
 *
 * record ZNodeData(String data, int version) {}
 * </pre>
 *
 * <h3>ZooKeeperException</h3>
 * <pre>
 * public class ZooKeeperException extends Exception {
 *     enum Code { NODE_EXISTS, NO_NODE, BAD_VERSION, NOT_EMPTY }
 *     public Code getCode() { ... }
 * }
 * </pre>
 *
 * <h3>RaftNode extension — pluggable state machine</h3>
 * Add a {@code RaftStateMachine} interface:
 * <pre>
 * public interface RaftStateMachine {
 *     void apply(String command);
 * }
 * </pre>
 * Add a second constructor to {@code RaftNode}:
 * <pre>
 * public RaftNode(String nodeId, int port, List&lt;String&gt; peerAddresses,
 *                 RaftStateMachine stateMachine)
 * </pre>
 * When this constructor is used, {@code applyEntries()} calls
 * {@code stateMachine.apply(command)} instead of the built-in KV logic.
 * The existing no-{@code RaftStateMachine} constructor continues to use the
 * built-in KV logic — all existing tests are unaffected.
 *
 * <h3>ZNodeTree (internal)</h3>
 * In-memory tree of ZNodes. Not thread-safe — called only from the
 * Raft state machine apply path (single-threaded per node).
 * <pre>
 * class ZNodeTree implements RaftStateMachine {
 *     // Map from path → ZNode
 *     // Map from path → List&lt;Consumer&lt;WatchEvent&gt;&gt; (data watches, from getData)
 *     // Map from path → List&lt;Consumer&lt;WatchEvent&gt;&gt; (exists watches, from exists)
 *     // Map from path → List&lt;Consumer&lt;WatchEvent&gt;&gt; (children watches, from getChildren)
 *     // Map from sessionId → List&lt;String&gt; (ephemeral node paths)
 *     // Map from parent path → AtomicInteger (sequential counter per parent prefix)
 *
 *     void apply(String command)   // parse and dispatch to create/delete/setData/sessionClose
 *     String  create(sessionId, path, data, mode, seqNumber)
 *     void    delete(path, version)
 *     void    setData(path, data, version)
 *     void    sessionClose(sessionId)
 *
 *     ZNodeData   getData(String path)
 *     boolean     exists(String path)
 *     List&lt;String&gt; getChildren(String path)
 *
 *     // Watch registration (called outside the state machine, on the read path)
 *     void watchData(String path, Consumer&lt;WatchEvent&gt; cb)
 *     void watchExists(String path, Consumer&lt;WatchEvent&gt; cb)
 *     void watchChildren(String path, Consumer&lt;WatchEvent&gt; cb)
 * }
 * </pre>
 * Watch semantics:
 * <ul>
 *   <li>Data watch (registered via {@code getData}) — fired on {@code setData} or {@code delete}.
 *   <li>Exists watch (registered via {@code exists}) — fired on {@code create} (CREATED)
 *       or {@code delete} (DELETED) of that path.
 *   <li>Children watch (registered via {@code getChildren}) — fired when a direct child
 *       of that path is created or deleted.
 *   <li>All watches are one-shot: fired once, then removed.
 *   <li>Watches are fired synchronously inside {@code apply()}.
 * </ul>
 *
 * <h3>ZooKeeperServer</h3>
 * <pre>
 * public class ZooKeeperServer {
 *
 *     // Creates an internal Raft cluster of {@code clusterSize} nodes starting at
 *     // {@code basePort}. Each RaftNode is constructed with a shared ZNodeTree.
 *     public ZooKeeperServer(int basePort, int clusterSize)
 *
 *     public void start()    // starts all RaftNodes; auto-creates root "/"
 *     public void stop()
 *
 *     // Writes — go through Raft (blocking until committed)
 *     public String  create(String sessionId, String path, String data, ZNodeMode mode)
 *                           throws ZooKeeperException
 *     public void    delete(String sessionId, String path, int version)
 *                           throws ZooKeeperException
 *     public void    setData(String sessionId, String path, String data, int version)
 *                           throws ZooKeeperException
 *     public void    closeSession(String sessionId)   // deletes all ephemeral nodes for session
 *
 *     // Reads — served from local ZNodeTree state (no Raft round-trip)
 *     public ZNodeData       getData(String path, Consumer&lt;WatchEvent&gt; watch)
 *                                    throws ZooKeeperException
 *     public boolean         exists(String path, Consumer&lt;WatchEvent&gt; watch)
 *     public List&lt;String&gt;    getChildren(String path, Consumer&lt;WatchEvent&gt; watch)
 *                                         throws ZooKeeperException
 * }
 * </pre>
 *
 * <h3>Command encoding (ZNodeTree.apply format)</h3>
 * <pre>
 *   create:{sessionId}:{path}:{mode}:{seqNumber}:{data}
 *   delete:{path}:{version}
 *   setData:{path}:{version}:{data}
 *   sessionClose:{sessionId}
 * </pre>
 * For sequential modes, the server pre-assigns the sequence number by atomically
 * incrementing a per-parent counter in {@code ZNodeTree} before writing to Raft
 * (safe because all writes go through the single Raft leader).
 * For non-sequential modes {@code seqNumber = 0}.
 *
 * <h3>Write routing</h3>
 * {@code ZooKeeperServer} keeps a reference to all {@code RaftNode}s.
 * Writes call {@code raftNode.write(command)} on the current leader.
 * If the node is not the leader, find one that is and retry.
 * {@code closeSession} triggers deletion of all ephemeral nodes for the session —
 * each deletion is a separate Raft write (or batch them into a single compound command).
 *
 * <h3>Root node</h3>
 * {@code "/"} is created automatically during {@code start()} and is permanent.
 * All top-level paths (e.g. {@code "/foo"}) are direct children of {@code "/"}.
 */
@Timeout(30)
class ZooKeeperTest {

    // Ports 7401–7403: reserved for ZooKeeper tests.
    static final int BASE_PORT = 7401;

    ZooKeeperServer zkServer;

    @AfterEach
    void tearDown() {
        if (zkServer != null) {
            try {
                zkServer.stop();
            } catch (Exception ignored) {
            }
        }
    }

    private ZooKeeperServer startServer() throws InterruptedException {
        ZooKeeperServer server = new ZooKeeperServer(BASE_PORT, 3);
        server.start();
        Thread.sleep(1_500); // allow Raft leader election to complete
        return server;
    }

    // ── basic CRUD ────────────────────────────────────────────────────────────

    /**
     * A persistent ZNode is readable after creation and retains its data.
     * The initial version is 0.
     */
    @Test
    void create_persistentNode_isReadable() throws Exception {
        zkServer = startServer();
        String created = zkServer.create("s1", "/mynode", "hello", ZNodeMode.PERSISTENT);
        assertEquals("/mynode", created);
        ZNodeData data = zkServer.getData("/mynode", null);
        assertEquals("hello", data.data());
        assertEquals(0, data.version());
    }

    /**
     * Creating a ZNode at an already-occupied path throws {@code NODE_EXISTS}.
     */
    @Test
    void create_existingPath_throwsNodeExists() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/dup", "v1", ZNodeMode.PERSISTENT);
        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.create("s1", "/dup", "v2", ZNodeMode.PERSISTENT));
        assertEquals(ZooKeeperException.Code.NODE_EXISTS, ex.getCode());
    }

    /**
     * Creating a ZNode whose parent does not exist throws {@code NO_NODE}.
     * The full parent path must be created first.
     */
    @Test
    void create_missingParent_throwsNoNode() throws Exception {
        zkServer = startServer();
        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.create("s1", "/missing/child", "data", ZNodeMode.PERSISTENT));
        assertEquals(ZooKeeperException.Code.NO_NODE, ex.getCode());
    }

    /**
     * {@code getData} on a non-existent path throws {@code NO_NODE}.
     */
    @Test
    void getData_missingNode_throwsNoNode() throws Exception {
        zkServer = startServer();
        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.getData("/ghost", null));
        assertEquals(ZooKeeperException.Code.NO_NODE, ex.getCode());
    }

    /**
     * {@code setData} updates the data and increments the version.
     * The caller must supply the current version; a mismatch throws {@code BAD_VERSION}.
     * Version {@code -1} acts as a wildcard and always succeeds.
     */
    @Test
    void setData_updatesDataAndIncrementsVersion() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/node", "original", ZNodeMode.PERSISTENT);
        assertEquals(0, zkServer.getData("/node", null).version());

        zkServer.setData("/node", "updated", 0);

        ZNodeData result = zkServer.getData("/node", null);
        assertEquals("updated", result.data());
        assertEquals(1, result.version());
    }

    /**
     * {@code setData} with the wrong version throws {@code BAD_VERSION}.
     * Version {@code -1} bypasses the check.
     */
    @Test
    void setData_wrongVersion_throwsBadVersion() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/node", "v1", ZNodeMode.PERSISTENT);

        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.setData("/node", "v2", 99));
        assertEquals(ZooKeeperException.Code.BAD_VERSION, ex.getCode());

        // version -1 = wildcard: must succeed regardless of current version
        assertDoesNotThrow(() -> zkServer.setData("/node", "v3", -1));
        assertEquals("v3", zkServer.getData("/node", null).data());
    }

    /**
     * Deleting a node removes it and updates its parent's children list.
     * Version {@code -1} skips the version check.
     */
    @Test
    void delete_removesNode() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/parent", "", ZNodeMode.PERSISTENT);
        zkServer.create("s1", "/parent/child", "data", ZNodeMode.PERSISTENT);
        assertTrue(zkServer.exists("/parent/child", null));

        zkServer.delete("s1", "/parent/child", -1);

        assertFalse(zkServer.exists("/parent/child", null));
        assertFalse(zkServer.getChildren("/parent", null).contains("child"));
    }

    /**
     * Deleting a node that still has children throws {@code NOT_EMPTY}.
     */
    @Test
    void delete_nodeWithChildren_throwsNotEmpty() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/parent", "", ZNodeMode.PERSISTENT);
        zkServer.create("s1", "/parent/child", "", ZNodeMode.PERSISTENT);

        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.delete("s1", "/parent", -1));
        assertEquals(ZooKeeperException.Code.NOT_EMPTY, ex.getCode());
    }

    /**
     * {@code delete} with the wrong version throws {@code BAD_VERSION}.
     */
    @Test
    void delete_wrongVersion_throwsBadVersion() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/node", "v1", ZNodeMode.PERSISTENT);
        zkServer.setData("/node", "v2", 0); // version is now 1

        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.delete("s1", "/node", 0)); // stale version
        assertEquals(ZooKeeperException.Code.BAD_VERSION, ex.getCode());
    }

    /**
     * {@code getChildren} returns the names (not full paths) of immediate children only.
     * Deep descendants are excluded.
     */
    @Test
    void getChildren_returnsImmediateChildNamesOnly() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/parent", "", ZNodeMode.PERSISTENT);
        zkServer.create("s1", "/parent/a", "", ZNodeMode.PERSISTENT);
        zkServer.create("s1", "/parent/b", "", ZNodeMode.PERSISTENT);
        zkServer.create("s1", "/parent/a/deep", "", ZNodeMode.PERSISTENT);

        List<String> children = zkServer.getChildren("/parent", null);
        assertEquals(2, children.size());
        assertTrue(children.containsAll(List.of("a", "b")));
        assertFalse(children.contains("deep"), "deep descendants must not appear in getChildren");
    }

    // ── watches ───────────────────────────────────────────────────────────────

    /**
     * A data watch registered via {@code getData} fires with {@code DATA_CHANGED}
     * when {@code setData} is called on that path.
     */
    @Test
    void watch_dataChanged_fires() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/watched", "v1", ZNodeMode.PERSISTENT);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<WatchEvent> received = new AtomicReference<>();
        zkServer.getData("/watched", event -> {
            received.set(event);
            latch.countDown();
        });

        zkServer.setData("/watched", "v2", 0);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "data watch must fire on setData");
        assertEquals("/watched", received.get().path());
        assertEquals(WatchEventType.DATA_CHANGED, received.get().type());
    }

    /**
     * Watches are one-shot: they fire exactly once and are then removed.
     * Subsequent changes to the same path do not re-trigger the watch.
     */
    @Test
    void watch_isOneShot() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/node", "v1", ZNodeMode.PERSISTENT);

        AtomicInteger fireCount = new AtomicInteger(0);
        zkServer.getData("/node", event -> {
            fireCount.incrementAndGet();
        });

        zkServer.setData("/node", "v2", 0); // fires the watch
        Thread.sleep(300);
        assertEquals(1, fireCount.get(), "watch must have fired exactly once");

        zkServer.setData("/node", "v3", 1); // watch already consumed
        Thread.sleep(300);
        assertEquals(1, fireCount.get(), "watch must not fire again after being consumed");
    }

    /**
     * An exists watch on an existing node fires with {@code DELETED} when the node is deleted.
     */
    @Test
    void watch_nodeDeleted_firesExistsWatch() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/node", "data", ZNodeMode.PERSISTENT);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<WatchEvent> received = new AtomicReference<>();
        zkServer.exists("/node", event -> {
            received.set(event);
            latch.countDown();
        });

        zkServer.delete("s1", "/node", -1);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "exists watch must fire on delete");
        assertEquals(WatchEventType.DELETED, received.get().type());
        assertEquals("/node", received.get().path());
    }

    /**
     * An exists watch on a non-existent node fires with {@code CREATED} when
     * the node is created. This is the mechanism for detecting a node coming into existence.
     */
    @Test
    void watch_nodeCreated_firesExistsWatch() throws Exception {
        zkServer = startServer();
        assertFalse(zkServer.exists("/new", null));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<WatchEvent> received = new AtomicReference<>();
        zkServer.exists("/new", event -> {
            received.set(event);
            latch.countDown();
        });

        zkServer.create("s1", "/new", "hello", ZNodeMode.PERSISTENT);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "exists watch must fire on create");
        assertEquals(WatchEventType.CREATED, received.get().type());
        assertEquals("/new", received.get().path());
    }

    /**
     * A children watch registered via {@code getChildren} fires when a direct
     * child of that path is created or deleted.
     */
    @Test
    void watch_childCreated_firesChildrenWatch() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/parent", "", ZNodeMode.PERSISTENT);

        CountDownLatch latch = new CountDownLatch(1);
        zkServer.getChildren("/parent", event -> {
            latch.countDown();
        });

        zkServer.create("s1", "/parent/child", "data", ZNodeMode.PERSISTENT);

        assertTrue(latch.await(2, TimeUnit.SECONDS), "children watch must fire on child create");
    }

    // ── ephemeral nodes ───────────────────────────────────────────────────────

    /**
     * An ephemeral node is visible while its session is open and automatically
     * deleted when {@code closeSession} is called.
     */
    @Test
    void ephemeralNode_deletedOnSessionClose() throws Exception {
        zkServer = startServer();
        zkServer.create("session-A", "/ephemeral", "alive", ZNodeMode.EPHEMERAL);
        assertTrue(zkServer.exists("/ephemeral", null));

        zkServer.closeSession("session-A");
        assertFalse(zkServer.exists("/ephemeral", null),
                "ephemeral node must be deleted when its owning session closes");
    }

    /**
     * All ephemeral nodes owned by a session are deleted when the session closes.
     * Persistent nodes created by the same session are unaffected.
     */
    @Test
    void closeSession_deletesAllEphemeralNodes_persistentNodesSurvive() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/e1", "a", ZNodeMode.EPHEMERAL);
        zkServer.create("s1", "/e2", "b", ZNodeMode.EPHEMERAL);
        zkServer.create("s1", "/p1", "c", ZNodeMode.PERSISTENT);

        zkServer.closeSession("s1");

        assertFalse(zkServer.exists("/e1", null), "/e1 must be deleted");
        assertFalse(zkServer.exists("/e2", null), "/e2 must be deleted");
        assertTrue(zkServer.exists("/p1", null), "/p1 must survive — it is persistent");
    }

    // ── sequential nodes ──────────────────────────────────────────────────────

    /**
     * Sequential nodes get a monotonically-increasing 10-digit zero-padded suffix
     * appended to the requested path. The returned path includes the suffix.
     * When sorted lexicographically, sequential nodes appear in creation order.
     */
    @Test
    void sequentialNodes_appendMonotonicallyIncreasing() throws Exception {
        zkServer = startServer();
        zkServer.create("s1", "/seq", "", ZNodeMode.PERSISTENT);

        String p1 = zkServer.create("s1", "/seq/node-", "a", ZNodeMode.PERSISTENT_SEQUENTIAL);
        String p2 = zkServer.create("s1", "/seq/node-", "b", ZNodeMode.PERSISTENT_SEQUENTIAL);
        String p3 = zkServer.create("s1", "/seq/node-", "c", ZNodeMode.PERSISTENT_SEQUENTIAL);

        assertTrue(p1.startsWith("/seq/node-"), "created path must start with the requested prefix");
        assertTrue(p2.startsWith("/seq/node-"));
        assertTrue(p3.startsWith("/seq/node-"));

        assertNotEquals(p1, p2, "sequential paths must be unique");
        assertNotEquals(p2, p3);

        // Lexicographic sort must preserve creation order
        List<String> sorted = new ArrayList<>(List.of(p1, p2, p3));
        Collections.sort(sorted);
        assertEquals(List.of(p1, p2, p3), sorted,
                "sequential node paths must sort in creation order");
    }

    // ── coordination patterns ─────────────────────────────────────────────────

    /**
     * Leader election via ephemeral nodes:
     * <ol>
     *   <li>The first session to create {@code /leader} wins.
     *   <li>Concurrent attempts see {@code NODE_EXISTS}.
     *   <li>When the leader session closes, the node is deleted and a new leader can emerge.
     * </ol>
     */
    @Test
    void leaderElection_usingEphemeralNode() throws Exception {
        zkServer = startServer();

        // Session A becomes leader by creating the ephemeral /leader node.
        zkServer.create("session-A", "/leader", "session-A", ZNodeMode.EPHEMERAL);
        assertEquals("session-A", zkServer.getData("/leader", null).data());

        // Session B cannot become leader while A's node exists.
        ZooKeeperException ex = assertThrows(ZooKeeperException.class,
                () -> zkServer.create("session-B", "/leader", "session-B", ZNodeMode.EPHEMERAL));
        assertEquals(ZooKeeperException.Code.NODE_EXISTS, ex.getCode());

        // Session B watches /leader so it knows when A's lease expires.
        CountDownLatch stepDownLatch = new CountDownLatch(1);
        zkServer.exists("/leader", event -> {
            stepDownLatch.countDown();
        });

        // Session A crashes (session closes → ephemeral node deleted).
        zkServer.closeSession("session-A");
        assertTrue(stepDownLatch.await(2, TimeUnit.SECONDS),
                "watch must fire when the leader's session closes");
        assertFalse(zkServer.exists("/leader", null));

        // Session B can now become the new leader.
        zkServer.create("session-B", "/leader", "session-B", ZNodeMode.EPHEMERAL);
        assertEquals("session-B", zkServer.getData("/leader", null).data());
    }

    /**
     * Distributed lock via sequential ephemeral nodes (no thundering herd):
     * <ol>
     *   <li>Each client creates an {@code EPHEMERAL_SEQUENTIAL} node under {@code /lock/}.
     *   <li>The client with the lowest-numbered node holds the lock.
     *   <li>Other clients watch only the node immediately preceding theirs — one client
     *       is woken per release, not all waiters.
     *   <li>When the lock holder's session closes, the watcher acquires the lock.
     * </ol>
     */
    @Test
    void distributedLock_sequentialEphemeral() throws Exception {
        zkServer = startServer();
        zkServer.create("admin", "/lock", "", ZNodeMode.PERSISTENT);

        // Session A and B each claim a lock slot.
        String lockA = zkServer.create("session-A", "/lock/req-", "", ZNodeMode.EPHEMERAL_SEQUENTIAL);
        String lockB = zkServer.create("session-B", "/lock/req-", "", ZNodeMode.EPHEMERAL_SEQUENTIAL);

        // A holds the lock — it has the lowest sequential node.
        List<String> children = zkServer.getChildren("/lock", null);
        Collections.sort(children);
        String lockAName = lockA.substring("/lock/".length());
        String lockBName = lockB.substring("/lock/".length());
        assertEquals(lockAName, children.get(0), "A must hold the lock (lowest node)");

        // B watches A's node — it will be woken exactly when A releases.
        CountDownLatch releaseLatch = new CountDownLatch(1);
        zkServer.exists(lockA, event -> {
            releaseLatch.countDown();
        });

        // A releases the lock by closing its session.
        zkServer.closeSession("session-A");
        assertTrue(releaseLatch.await(2, TimeUnit.SECONDS),
                "B's watch must fire when A releases the lock");

        // B now holds the lock — it has the only remaining node.
        children = zkServer.getChildren("/lock", null);
        Collections.sort(children);
        assertEquals(1, children.size(), "only B's node should remain");
        assertEquals(lockBName, children.get(0), "B must now hold the lock");
    }
}
