package org.example.rpc;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.example.rpc.ZooKeeperException.Code;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Log4j2
public class ZooKeeperServer {
    private static final String SLASH = "/";
    private final List<RaftNode> peers;

    public ZooKeeperServer(int port, int clusterSize) {
        List<String> addrs = new ArrayList<>();
        for (int i1 = 0; i1 < clusterSize; i1++) {
            addrs.add("localhost:" + (port + i1));
        }
        List<RaftNode> nodes = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            List<String> peers1 = new ArrayList<>(addrs); // create a copy
            peers1.remove(i); // a node does not list itself as a peer
            RaftNode node = new RaftNode("node-" + i, port + i, peers1, new ZNodeTree(SLASH));
            nodes.add(node);
        }
        peers = nodes;
    }

    public void start() {
        peers.forEach(RaftNode::start);
        log.info("ZooKeeperServer started");
    }

    public void stop() {
        peers.forEach(RaftNode::stop);
        log.info("ZooKeeperServer stopped");
    }

    // Writes — go through Raft (blocking until committed)
    @SneakyThrows
    public String create(String sessionId, String path, String data, ZNodeMode mode) throws ZooKeeperException {
        RaftNode leader = getLeader();
        ZNodeTree tree = leader.getZNodeTree();
        String parent = path.substring(0, path.lastIndexOf(SLASH));
        if (!parent.isEmpty() && !tree.exists(parent)) {
            throw new ZooKeeperException(Code.NO_NODE, parent);
        }

        String actualPath = path;
        if (mode == ZNodeMode.PERSISTENT_SEQUENTIAL || mode == ZNodeMode.EPHEMERAL_SEQUENTIAL) {
            String seqParent = parent.isEmpty() ? SLASH : parent;
            int seq = tree.getAndIncrementSeqNumber(seqParent);
            actualPath = path + String.format("%010d", seq);
        } else if (tree.exists(path)) {
            throw new ZooKeeperException(Code.NODE_EXISTS, path);
        }

        String createCommand = MessageFormat.format("create:{0}:{1}:{2}:{3}:{4}", sessionId, actualPath, mode, 0, data);
        leader.write(createCommand).get();
        return actualPath;
    }

    @SneakyThrows
    public void delete(String sessionId, String path, int version) throws ZooKeeperException {
        RaftNode leader = getLeader();
        ZNodeTree tree = leader.getZNodeTree();
        String parent = path.substring(0, path.lastIndexOf(SLASH));
        if (!parent.isEmpty() && !tree.exists(parent)) {
            throw new ZooKeeperException(Code.NO_NODE, parent);
        }
        // Pre-validate before Raft write — applyEntries() swallows ZooKeeperException
        ZNodeData nodeData = tree.getData(path);
        if (version != -1 && nodeData.version() != version) {
            throw new ZooKeeperException(Code.BAD_VERSION,
                    "expected version " + nodeData.version() + " but got " + version);
        }
        if (!tree.getChildren(path).isEmpty()) {
            throw new ZooKeeperException(Code.NOT_EMPTY, path);
        }
        String deleteCommand = MessageFormat.format("delete:{0}:{1}", path, version);
        leader.write(deleteCommand).get();
    }

    @SneakyThrows
    public void setData(String path, String data, int version) throws ZooKeeperException {
        RaftNode leader = getLeader();
        ZNodeTree tree = leader.getZNodeTree();
        String parent = path.substring(0, path.lastIndexOf(SLASH));
        if (!parent.isEmpty() && !tree.exists(parent)) {
            throw new ZooKeeperException(Code.NO_NODE, parent);
        }
        // Pre-validate version before Raft write — applyEntries() swallows ZooKeeperException
        ZNodeData nodeData = tree.getData(path);
        if (version != -1 && nodeData.version() != version) {
            throw new ZooKeeperException(Code.BAD_VERSION,
                    "expected version " + nodeData.version() + " but got " + version);
        }
        String setDataCommand = MessageFormat.format("setData:{0}:{1}:{2}", path, version, data);
        leader.write(setDataCommand).get();
    }

    // Deletes all ephemeral nodes for session
    @SneakyThrows
    public void closeSession(String sessionId) throws ZooKeeperException {
        RaftNode leader = getLeader();
        String closeSessionCommand = MessageFormat.format("sessionClose:{0}", sessionId);
        leader.write(closeSessionCommand).get();
    }

    public ZNodeData getData(String path, Consumer<WatchEvent> watch) throws ZooKeeperException {
        ZNodeTree zNodeTree = getLeader().getZNodeTree();
        ZNodeData data = zNodeTree.getData(path); // throws NO_NODE if missing
        zNodeTree.watchData(path, watch);
        return data;
    }

    public boolean exists(String path, Consumer<WatchEvent> watch) throws ZooKeeperException {
        ZNodeTree zNodeTree = getLeader().getZNodeTree();
        boolean result = zNodeTree.exists(path);
        zNodeTree.watchExists(path, watch);
        return result;
    }

    public List<String> getChildren(String path, Consumer<WatchEvent> watch) throws ZooKeeperException {
        ZNodeTree zNodeTree = getLeader().getZNodeTree();
        zNodeTree.watchChildren(path, watch);
        return zNodeTree.getChildren(path);
    }

    private RaftNode getLeader() throws ZooKeeperException {
        for (RaftNode peer : peers) {
            if (peer.isLeader()) {
                log.info("leader: {}", peer.getNodeId());
                return peer;
            }
        }
        throw new RuntimeException("no leader");
    }
}
