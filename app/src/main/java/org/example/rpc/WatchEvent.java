package org.example.rpc;

public record WatchEvent(String path, WatchEventType type) {}