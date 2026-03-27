package org.example.rpc;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;

public class HealthMonitor {
    private final ConcurrentHashMap<String, Instant> heartbeats;
    private final long timeOut;

    public HealthMonitor(long tickIntervalMs, int maxMissedTicks) {
        timeOut = tickIntervalMs * maxMissedTicks;
        heartbeats = new ConcurrentHashMap<>();
    }

    public void register(String nodeId) {
        heartbeats.put(nodeId, Instant.now());
    }

    public void close() {

    }

    public NodeStatus getStatus(String nodeId) {
        Instant last = heartbeats.get(nodeId);
        if (last == null) {
            throw new IllegalArgumentException("unregistered node: %s".formatted(nodeId));
        }
        boolean isStale = last.plus(timeOut, ChronoUnit.MILLIS).isBefore(Instant.now());
        if (isStale) {
            return NodeStatus.SUSPECTED_DOWN;
        }
        return NodeStatus.HEALTHY;
    }

    public void heartbeat(String nodeId) {
        heartbeats.compute(nodeId, (k, instant) -> {
            if (instant == null) {
                throw new IllegalArgumentException("unregistered node: %s".formatted(nodeId));
            }
            return Instant.now();
        });
    }
}
