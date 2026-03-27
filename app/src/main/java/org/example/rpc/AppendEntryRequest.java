package org.example.rpc;

import java.util.ArrayList;
import java.util.List;

public record AppendEntryRequest(
        int term,
        String leaderId,
        int prevLogIndex,
        int prevLogTerm,
        int leaderCommit,
        List<String> entries
) implements RaftRequest {
    public static AppendEntryRequest decode(List<String> args) {
        return new AppendEntryRequest(Integer.parseInt(args.get(0)),                                     // term
                args.get(1),                                                    // leaderId
                Integer.parseInt(args.get(2)),                                  // prevLogIndex
                Integer.parseInt(args.get(3)),                                  // prevLogTerm
                Integer.parseInt(args.get(4)),                                  // leaderCommit
                args.size() > 5 ? args.subList(5, args.size()) : List.of());    // entries
    }

    @Override
    public List<String> encode() {
        List<String> list = new ArrayList<>();
        list.add(String.valueOf(term));
        list.add(leaderId);
        list.add(String.valueOf(prevLogIndex));
        list.add(String.valueOf(prevLogTerm));
        list.add(String.valueOf(leaderCommit));
        list.addAll(entries);
        return list;
    }
}
