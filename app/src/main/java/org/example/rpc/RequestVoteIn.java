package org.example.rpc;

import java.util.List;

public record RequestVoteIn(
        int term,
        String candidateId,
        int lastLogIndex,
        int lastLogTerm
) {
    List<String> encode() {
        return List.of(Integer.toString(term),
                candidateId,
                String.valueOf(lastLogIndex),
                String.valueOf(lastLogTerm));
    }

    public static RequestVoteIn decode(List<String> args) {
        return new RequestVoteIn(Integer.parseInt(args.get(0)),
                args.get(1),
                Integer.parseInt(args.get(2)),
                Integer.parseInt(args.get(3)));
    }
}
