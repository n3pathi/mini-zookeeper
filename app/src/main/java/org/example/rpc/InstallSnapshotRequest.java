package org.example.rpc;

import java.util.ArrayList;
import java.util.List;

public record InstallSnapshotRequest(
        int term,
        String leaderId,
        int lastIncludedIndex,
        int lastIncludedTerm,
        List<String> data
) implements RaftRequest {
    public static InstallSnapshotRequest decode(List<String> args) {
        int term = Integer.parseInt(args.get(0));
        String leaderId = args.get(1);
        int lastIncludedIndex = Integer.parseInt(args.get(2));
        int lastIncludedTerm = Integer.parseInt(args.get(3));
        List<String> data = new ArrayList<>(); //entry format key:value
        if (args.size() > 4) {
            for (int i = 4; i < args.size(); i++) {
                data.add(args.get(i));
            }
        }
        return new InstallSnapshotRequest(term, leaderId, lastIncludedIndex, lastIncludedTerm, data);
    }

    @Override
    public List<String> encode() {
        List<String> list = new ArrayList<>();
        list.add(String.valueOf(term));
        list.add(leaderId);
        list.add(String.valueOf(lastIncludedIndex));
        list.add(String.valueOf(lastIncludedTerm));
        list.addAll(data);
        return list;
    }
}
