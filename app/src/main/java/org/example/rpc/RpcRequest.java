package org.example.rpc;

import java.util.List;

public record RpcRequest(String id, String method, List<String> args) {
}
