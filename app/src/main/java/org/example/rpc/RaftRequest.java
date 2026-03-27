package org.example.rpc;

import java.util.List;

public interface RaftRequest {
    List<String> encode();
}
