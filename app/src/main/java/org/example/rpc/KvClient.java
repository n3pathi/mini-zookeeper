package org.example.rpc;

import java.util.Optional;

public interface KvClient {
    void put(String key, String value);

    Optional<String> get(String key);

    void delete(String key);

    void close();
}
