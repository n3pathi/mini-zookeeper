package org.example.rpc;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class SimpleKvClient implements KvClient {

    private static final String NULL = "\0";
    private final RpcClient client;

    public SimpleKvClient(String host, int port) {
        client = new RpcClient(host, port);
    }

    @Override
    public void put(String key, String value) {
        try {
            client.call("put", List.of(key, value)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<String> get(String key) {
        try {
            RpcResponse response = client.call("get", List.of(key)).get();
            String result = response.result();
            if (!NULL.equals(result)) {
                return Optional.of(result);
            }
            return Optional.empty();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return Optional.empty();
    }

    @Override
    public void delete(String key) {
        try {
            client.call("delete", List.of(key)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
