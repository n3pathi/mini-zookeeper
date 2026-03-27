package org.example.rpc;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LeasedPrimaryKvServer {
    private final String nodeId;
    private final LeaseManager lm;
    private RpcServer server;
    private List<SimpleKvClient> clients;
    private ConcurrentHashMap<String, String> store;
    private final String NULL = "\0";
    private final String EMPTY = "";

    public LeasedPrimaryKvServer(int port, List<String> endpoints, String nodeId, LeaseManager lm) {
        server = new RpcServer(port);
        clients = endpoints.stream()
                .map(s -> s.split(":", 2))
                .map(sa -> new SimpleKvClient(sa[0], Integer.parseInt(sa[1])))
                .toList();
        this.nodeId = nodeId;
        this.lm = lm;
        this.store = new ConcurrentHashMap<>();

        server.register("get", args -> store.getOrDefault(args.getFirst(), NULL));
        server.register("put", args -> {
            if (!lm.isLeader(nodeId)) {
                return "NOT_LEADER";
            }
            store.put(args.getFirst(), args.get(1));
            clients.forEach(kvClient -> {
                try {
                    kvClient.put(args.getFirst(), args.get(1));
                } catch (Exception e) {
                    System.err.printf("replication error: %s%n", e.getMessage());
                }
            });
            return EMPTY;
        });
        server.register("delete", args -> {
            if (!lm.isLeader(nodeId)) {
                return "NOT_LEADER";
            }
            store.remove(args.getFirst());
            clients.forEach(kvClient -> {
                try {
                    kvClient.delete(args.getFirst());
                } catch (Exception e) {
                    System.err.printf("delete error: %s%n", e.getMessage());
                }
            });
            return EMPTY;
        });
    }

    public void close() {
        clients.forEach(client -> {
            try {
                client.close();
            } catch (Exception ignored) {
            }
        });
        server.close();
    }

    public void start() {
        server.start();
    }
}
