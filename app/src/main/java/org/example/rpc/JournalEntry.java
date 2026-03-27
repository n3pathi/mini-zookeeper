package org.example.rpc;

public record JournalEntry(
        int index,
        int term,
        String command
) {
    public static JournalEntry decode(String s) {
        String[] tokens = s.split(":", 3);
        return new JournalEntry(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]), tokens[2]);
    }

    public String encode() {
        return String.format("%d:%d:%s", index, term, command);
    }
}
