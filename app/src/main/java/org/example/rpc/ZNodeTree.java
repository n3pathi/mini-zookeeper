package org.example.rpc;

import lombok.extern.log4j.Log4j2;
import org.example.rpc.ZooKeeperException.Code;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Log4j2
public class ZNodeTree implements RaftStateMachine {
    private static final String COLON = ":";
    private static final String SLASH = "/";

    // Map from path → ZNode
    private final Map<String, ZNodeTree> children;
    // Map from path → List<Consumer<WatchEvent>> (exists watches, from exists)
    private final Map<String, List<Consumer<WatchEvent>>> existsWatches;
    // Map from path → List<Consumer<WatchEvent>> (data watches, from getData)
    private final Map<String, List<Consumer<WatchEvent>>> dataWatches;
    // Map from path → List<Consumer<WatchEvent>> (children watches, from getChildren)
    private final Map<String, List<Consumer<WatchEvent>>> childrenWatches;
    // Map from sessionId → List<String> (ephemeral node paths)
    private final Map<String, List<String>> ephemeralNodes;
    // Map from parent path → AtomicInteger (sequential counter per parent prefix)
    private final Map<String, AtomicInteger> sequentialCounters;

    private final String zNode;
    private ZNodeData zNodeData;

    public ZNodeTree(String name) {
        zNode = name;
        children = new HashMap<>();
        existsWatches = new HashMap<>();
        dataWatches = new HashMap<>();
        childrenWatches = new HashMap<>();
        ephemeralNodes = new HashMap<>();
        sequentialCounters = new HashMap<>();
    }

    // parse and dispatch to create/delete/setData/sessionClose
    public void apply(String command) throws ZooKeeperException {
        log.trace("apply command: {}", command);
        Command c = Command.from(command.substring(0, command.indexOf(COLON)));
        switch (c) {
            case CREATE -> {
                // create:{sessionId}:{path}:{mode}:{seqNumber}:{data}
                String[] tokens = command.split(COLON, 6);
                String sessionId = tokens[1];
                String path = tokens[2];
                ZNodeMode mode = ZNodeMode.valueOf(tokens[3]);
                int seqNumber = Integer.parseInt(tokens[4]);
                String data = tokens[5];
                create(sessionId, path, data, mode, seqNumber);
            }
            case DELETE -> {
                // delete:{path}:{version}
                String[] tokens = command.split(COLON, 3);
                String path = tokens[1];
                int version = Integer.parseInt(tokens[2]);
                delete(path, version);
            }
            case SESSION_CLOSE -> {
                // sessionClose:{sessionId}
                String[] tokens = command.split(COLON, 2);
                String sessionId = tokens[1];
                sessionClose(sessionId);
            }
            case SET_DATA -> {
                // setData:{path}:{version}:{data}
                String[] tokens = command.split(COLON, 4);
                String path = tokens[1];
                int version = Integer.parseInt(tokens[2]);
                String data = tokens[3];
                setData(path, data, version);
            }
            default -> throw new IllegalArgumentException("Unknown command: " + c);
        }
    }

    // create: validate at root, insert via private helper, then track ephemeral & fire watches
    String create(String sessionId, String path, String data, ZNodeMode mode, int seqNumber) throws ZooKeeperException {
        String parent = getParent(path);
        if (parent.isEmpty() || !exists(parent)) {
            throw new ZooKeeperException(Code.NO_NODE, "no such node: " + path);
        }
        if (exists(path)) {
            throw new ZooKeeperException(Code.NODE_EXISTS, path);
        }
        insertNode(path, data, seqNumber);

        if (mode == ZNodeMode.EPHEMERAL || mode == ZNodeMode.EPHEMERAL_SEQUENTIAL) {
            ephemeralNodes.computeIfAbsent(sessionId, k -> new ArrayList<>()).add(path);
        }

        WatchEvent createdEvent = new WatchEvent(path, WatchEventType.CREATED);
        if (existsWatches.containsKey(path)) {
            existsWatches.remove(path).forEach(cb -> cb.accept(createdEvent));
        }
        if (childrenWatches.containsKey(parent)) {
            childrenWatches.remove(parent).forEach(cb -> cb.accept(createdEvent));
        }

        return path;
    }

    // Recursively inserts a new node without any side effects (no watch firing, no ephemeral tracking)
    private void insertNode(String path, String data, int seqNumber) {
        String parent = getParent(path);
        if (parent.equals(zNode)) {
            ZNodeTree child = new ZNodeTree(path);
            child.zNodeData = new ZNodeData(data, seqNumber);
            children.put(path, child);
            log.debug("created node: {}", path);
        } else {
            int endIndex = path.indexOf(SLASH, zNode.length() + 1);
            String key = path.substring(0, endIndex);
            children.get(key).insertNode(path, data, seqNumber);
        }
    }

    void delete(String path, int version) throws ZooKeeperException {
        String parent = getParent(path);
        if (parent.isEmpty() || !exists(parent)) {
            throw new ZooKeeperException(Code.NO_NODE, "no such node: " + parent);
        }
        if (parent.equals(zNode)) {
            for (Map.Entry<String, ZNodeTree> e : children.entrySet()) {
                if (e.getKey().equals(path)) {
                    getZNodeTree(version, e); // validates version and not-empty
                    break;
                }
            }
            children.remove(path);
        } else {
            int endIndex = path.indexOf(SLASH, zNode.length() + 1);
            String key = path.substring(0, endIndex);
            children.get(key).delete(path, version);
        }
        WatchEvent event = new WatchEvent(path, WatchEventType.DELETED);
        if (dataWatches.containsKey(path)) {
            dataWatches.remove(path).forEach(consumer -> consumer.accept(event));
        }
        if (existsWatches.containsKey(path)) {
            existsWatches.remove(path).forEach(consumer -> consumer.accept(event));
        }
        if (childrenWatches.containsKey(parent)) {
            childrenWatches.remove(parent).forEach(consumer -> consumer.accept(event));
        }
    }

    private static @NonNull ZNodeTree getZNodeTree(int version, Map.Entry<String, ZNodeTree> e) throws ZooKeeperException {
        ZNodeTree value = e.getValue();
        int existingVersion = value.zNodeData.version();
        if ((-1 != version) && (existingVersion != version)) {
            throw new ZooKeeperException(Code.BAD_VERSION, "expected version " + existingVersion + " but got " + version);
        }
        if (!value.children.isEmpty()) {
            throw new ZooKeeperException(Code.NOT_EMPTY, "not empty: " + value.zNode);
        }
        return value;
    }

    void setData(String path, String data, int version) throws ZooKeeperException {
        String parent = getParent(path);
        if (parent.isEmpty() || !exists(parent)) {
            throw new ZooKeeperException(Code.NO_NODE, "no such node: " + path);
        }
        if (parent.equals(zNode)) {
            ZNodeTree zNodeTree = children.get(path);
            int existingVersion = zNodeTree.zNodeData.version();
            if ((-1 != version) && (existingVersion != version)) {
                throw new ZooKeeperException(Code.BAD_VERSION, "expected version " + existingVersion + " but got " + version);
            }
            zNodeTree.zNodeData = new ZNodeData(data, existingVersion + 1);
            log.debug("set data {} to {} with version {}", data, path, version);
        } else {
            int endIndex = path.indexOf(SLASH, zNode.length() + 1);
            String key = path.substring(0, endIndex);
            children.get(key).setData(path, data, version);
        }
        WatchEvent watchEvent = new WatchEvent(path, WatchEventType.DATA_CHANGED);
        if (dataWatches.containsKey(path)) {
            dataWatches.remove(path).forEach(consumer -> consumer.accept(watchEvent));
        }
    }

    // Deletes all ephemeral nodes owned by sessionId
    void sessionClose(String sessionId) {
        List<String> paths = ephemeralNodes.remove(sessionId);
        if (paths != null) {
            for (String path : new ArrayList<>(paths)) {
                try {
                    delete(path, -1);
                } catch (ZooKeeperException e) {
                    log.warn("failed to delete ephemeral node {} on session close: {}", path, e.getMessage());
                }
            }
        }
        log.info("session {} closed", sessionId);
    }

    ZNodeData getData(String path) throws ZooKeeperException {
        if (path.equals(zNode)) {
            return zNodeData;
        }
        if (!exists(path)) {
            throw new ZooKeeperException(Code.NO_NODE, path);
        }
        for (Map.Entry<String, ZNodeTree> e : children.entrySet()) {
            String key = e.getKey();
            if (path.equals(key) || path.startsWith(key + SLASH)) {
                return e.getValue().getData(path);
            }
        }
        throw new ZooKeeperException(Code.NO_NODE, path);
    }

    boolean exists(String path) {
        if (path.equals(zNode)) {
            return true;
        }
        return children.entrySet().stream()
                .filter(e -> path.equals(e.getKey()) || path.startsWith(e.getKey() + SLASH))
                .findFirst()
                .map(e -> e.getValue().exists(path))
                .orElse(false);
    }

    // Returns child names (last path segment) for the given path
    List<String> getChildren(String path) {
        if (path.equals(zNode)) {
            List<String> names = new ArrayList<>();
            for (String k : children.keySet()) {
                names.add(k.substring(k.lastIndexOf(SLASH) + 1));
            }
            return names;
        }
        for (Map.Entry<String, ZNodeTree> e : children.entrySet()) {
            String key = e.getKey();
            if (path.equals(key) || path.startsWith(key + SLASH)) {
                return e.getValue().getChildren(path);
            }
        }
        return new ArrayList<>();
    }

    // Watch registration (called outside the state machine, on the read path)
    // All watches are stored at the root level — no tree traversal needed.
    void watchData(String path, Consumer<WatchEvent> cb) {
        if (cb != null) dataWatches.computeIfAbsent(path, k -> new ArrayList<>()).add(cb);
    }

    void watchExists(String path, Consumer<WatchEvent> cb) {
        if (cb != null) existsWatches.computeIfAbsent(path, k -> new ArrayList<>()).add(cb);
    }

    void watchChildren(String path, Consumer<WatchEvent> cb) {
        if (cb != null) childrenWatches.computeIfAbsent(path, k -> new ArrayList<>()).add(cb);
    }

    // Returns the next sequence number for a given parent path (used by ZooKeeperServer for sequential nodes)
    int getAndIncrementSeqNumber(String parent) {
        return sequentialCounters.computeIfAbsent(parent, k -> new AtomicInteger(0)).incrementAndGet();
    }

    private static @NonNull String getParent(String path) {
        int lastIndexOfSlash = path.lastIndexOf(SLASH);
        if (lastIndexOfSlash == 0) {
            return SLASH;
        } else if (lastIndexOfSlash <= 0) {
            return "";
        }
        return path.substring(0, lastIndexOfSlash);
    }
}
