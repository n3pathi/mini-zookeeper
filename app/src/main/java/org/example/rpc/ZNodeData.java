package org.example.rpc;

import java.util.StringJoiner;

public record ZNodeData(String data, int version) {
    @Override
    public String toString() {
        return new StringJoiner(", ", ZNodeData.class.getSimpleName() + "[", "]")
                .add("data='" + data + "'")
                .add("version=" + version)
                .toString();
    }
}