package org.example.rpc;

import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class KvServer {
    private static final String NULL = "\0";
    private static final String EMPTY = "";

    private final int port;
    private final String nodeId;

    private RpcServer server;
    private final HealthMonitor healthMonitor;
    private final ConcurrentHashMap<String, String> store;

    public KvServer(int port) {
        this(port, null, null);
    }

    public KvServer(int port,
                    String nodeId,
                    HealthMonitor healthMonitor) {
        this.port = port;
        this.nodeId = nodeId;
        store = new ConcurrentHashMap<>();
        this.healthMonitor = healthMonitor;
    }

    private @NonNull Function<List<String>, String> handleDelete() {
        return args -> {
            store.remove(args.getFirst());
            return EMPTY;
        };
    }

    private @NonNull Function<List<String>, String> handleGet() {
        return args -> store.getOrDefault(args.getFirst(), NULL);
    }

    private @NonNull Function<List<String>, String> handlePut() {
        return args -> {
            store.put(args.getFirst(), args.get(1));
            return EMPTY;
        };
    }

    public void start() {
        server = new RpcServer(port);
        server.register("put", handlePut());
        server.register("get", handleGet());
        server.register("delete", handleDelete());
        if (null != healthMonitor) {
            this.healthMonitor.register(nodeId);
        }
        server.start();
    }

    public void close() {
        server.close();
    }
}
