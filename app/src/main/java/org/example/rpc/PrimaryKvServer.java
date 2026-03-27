package org.example.rpc;

import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class PrimaryKvServer {
    private static final String NULL = "\0";
    private static final String EMPTY = "";
    private final RpcServer server;
    private final List<KvClient> clients;
    private final ConcurrentHashMap<String, String> store;
    private final HealthMonitor monitor;

    public PrimaryKvServer(int port,
                           List<String> replicaSevers) {
        this(port, replicaSevers, null, null);
    }

    public PrimaryKvServer(int port,
                           List<String> replicaSevers,
                           String nodeId,
                           HealthMonitor monitor) {
        server = new RpcServer(port);
        clients = new ArrayList<>();
        store = new ConcurrentHashMap<>();
        this.monitor = monitor;
        if (null != monitor) {
            monitor.register(nodeId);
        }

//
//        register rpc methods
//
        for (String hostPort : replicaSevers) {
            String[] tokens = hostPort.split(":", 2);
            String host = tokens[0];
            int rPort = Integer.parseInt(tokens[1]);
            clients.add(new SimpleKvClient(host, rPort));
        }
        server.register("put", handlePut());
        server.register("get", handleGet());
        server.register("delete", handleDelete());
    }

    private @NonNull Function<List<String>, String> handleDelete() {
        return args -> {
            store.remove(args.getFirst());
            clients.forEach(kvClient -> {
                try {
                    kvClient.delete(args.getFirst());
                } catch (Exception e) {
                    System.err.printf("delete error: %s%n", e.getMessage());
                }
            });
            return EMPTY;
        };
    }

    private @NonNull Function<List<String>, String> handleGet() {
        return args -> store.getOrDefault(args.getFirst(), NULL);
    }

    private @NonNull Function<List<String>, String> handlePut() {
        return args -> {
            store.put(args.getFirst(), args.get(1));
            clients.forEach(kvClient -> {
                try {
                    kvClient.put(args.getFirst(), args.get(1));
                } catch (Exception e) {
                    System.err.printf("replication error: %s%n", e.getMessage());
                }
            });
            return EMPTY;
        };
    }

    public void start() {
        server.start();
    }

    public void close() {
        server.close();
    }
}
