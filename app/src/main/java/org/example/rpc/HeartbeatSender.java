package org.example.rpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HeartbeatSender {
    private final String nodeId;
    private final HealthMonitor healthMonitor;
    private final ScheduledExecutorService scheduledExecutorService;

    public HeartbeatSender(String nodeId,
                           HealthMonitor healthMonitor,
                           ScheduledExecutorService scheduledExecutorService) {
        this.nodeId = nodeId;
        this.healthMonitor = healthMonitor;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public HeartbeatSender(String nodeId,
                           HealthMonitor healthMonitor,
                           long intervalMs) {
        this.nodeId = nodeId;
        this.healthMonitor = healthMonitor;
        this.healthMonitor.register(nodeId);
        this.scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduleTask(intervalMs);
    }

    public void scheduleTask(long intervalMs) {
        scheduledExecutorService.scheduleAtFixedRate(this::sendHeartbeat,
                0,
                intervalMs,
                TimeUnit.MILLISECONDS);
    }

    public void sendHeartbeat() {
        healthMonitor.heartbeat(nodeId);
    }

    public void close() {
        scheduledExecutorService.shutdownNow();
    }
}
