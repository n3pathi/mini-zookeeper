package org.example.rpc;

import lombok.Getter;

import java.util.EnumSet;

public enum Command {
    CREATE("create"),
    DELETE("delete"),
    SESSION_CLOSE("sessionClose"),
    SET_DATA("setData"),
    ;

    Command(String name) {
        this.name = name;
    }

    @Getter
    private final String name;

    public static Command from(String name) {
        EnumSet<Command> commands = EnumSet.allOf(Command.class);
        for (Command command : commands) {
            if (command.name.equals(name)) {
                return command;
            }
        }
        throw new IllegalArgumentException("Unknown command: " + name);
    }
}
