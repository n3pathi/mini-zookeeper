package org.example.rpc;

public record AppendEntryResponse(
        int term,
        boolean success,
        int matchIndex
) {
    public AppendEntryResponse(String s) {
        String[] tokens = s.split(":", 3);
        this(Integer.parseInt(tokens[0]),
                Boolean.parseBoolean(tokens[1]),
                Integer.parseInt(tokens[2]));
    }

    public String encode() {
        return String.format("%d:%s:%d", term, success, matchIndex);
    }
}
