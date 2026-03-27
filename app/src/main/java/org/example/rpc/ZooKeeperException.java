package org.example.rpc;

import lombok.Getter;

public class ZooKeeperException extends Exception {


    public enum Code {NODE_EXISTS, NO_NODE, BAD_VERSION, NOT_EMPTY;}
    @Getter
    private Code code;

    public ZooKeeperException(Code code, String message) {
        super(message);
        this.code = code;
    }
}
