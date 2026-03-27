package org.example.rpc;

public record RequestVoteOut(int term, boolean support) {
    public String encode() {
        return "%d,%s".formatted(term, support);
    }

    public static RequestVoteOut decode(String s) {
        String[] tokens = s.split(",", 2);
        int term = Integer.parseInt(tokens[0]);
        boolean support = Boolean.parseBoolean(tokens[1]);
        return new RequestVoteOut(term, support);
    }
}
