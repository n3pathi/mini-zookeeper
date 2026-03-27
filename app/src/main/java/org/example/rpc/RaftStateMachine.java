package org.example.rpc;

public interface RaftStateMachine {
    void apply(String command) throws ZooKeeperException;
}
