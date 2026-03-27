package org.example.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class FailoverKvClient implements KvClient {

    private final List<SimpleKvClient> clients;
    private final AtomicInteger primary;
    private final int maxAttempts;

    public FailoverKvClient(List<String> endpoints) {
        clients = new ArrayList<>();
        endpoints.forEach(endpoint -> {
            String[] tokens = endpoint.split(":", 2);
            String host = tokens[0];
            int port = Integer.parseInt(tokens[1]);
            clients.add(new SimpleKvClient(host, port));
        });
        primary = new AtomicInteger(0);
        maxAttempts = endpoints.size() / 2 + 1;
    }

    @Override
    public void put(String key, String value) {
        withFailOver(client -> client.put(key, value));
    }

    private SimpleKvClient getPrimaryClient() {
        return clients.get(primary.get());
    }

    @Override
    public Optional<String> get(String key) {
        return withFailover(client -> client.get(key));
    }

    @Override
    public void delete(String key) {
        withFailOver(client -> client.delete(key));
    }

    @Override
    public void close() {
        clients.forEach(client -> {
            try {
                client.close();
            } catch (Exception e) {
                System.err.printf("error closing client %s with %s%n", client, e.getMessage());
            }
        });
    }

    private <T> T withFailover(Function<SimpleKvClient, T> fn) {
        RuntimeException lastException = null;
        for (int i = 0; i < maxAttempts; i++) {
            try {
                return fn.apply(getPrimaryClient());
            } catch (RuntimeException e) {
                lastException = e;
                failoverClient();
            }
        }
        throw new RuntimeException(lastException);
    }

    private void failoverClient() {
        int expected = primary.get();
        int newPrimary = (expected + 1) % clients.size();
        System.out.printf("attempt to failover client %s -> %s%n", expected, newPrimary);
        primary.compareAndSet(expected, newPrimary);
    }

    private <T> void withFailOver(Consumer<SimpleKvClient> fn) {
        withFailover(client -> {
            fn.accept(client);
            return null;
        });
    }
}
