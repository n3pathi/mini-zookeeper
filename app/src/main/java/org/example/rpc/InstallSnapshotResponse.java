package org.example.rpc;

public record InstallSnapshotResponse(int term) {
    public static InstallSnapshotResponse decode(String s) {
        return new InstallSnapshotResponse(Integer.parseInt(s));
    }
}
