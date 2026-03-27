package org.example.rpc;

import java.time.Instant;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LeaseManager {

    private final long leaseDurationMs;
    private volatile long expireMs;
    private volatile String holder;
    private boolean closed;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LeaseManager(long leaseDurationMs) {
        this.leaseDurationMs = leaseDurationMs;
        closed = false;
    }

    // For the split-brain demo test only — forces a node to be leader without going through tryAcquire.
    public void forceLeader(String nodeId) {
        if (lock.writeLock().tryLock()) {
            try {
                holder = nodeId;
                expireMs = Instant.now().plusMillis(leaseDurationMs).toEpochMilli();
            } finally {
                lock.writeLock().unlock();
            }
        }
    }

    // Acquires lease if none is held or current one is expired.
    // Returns true if acquired. Returns false if closed.
    public boolean tryAcquire(String nodeId) {
        if (lock.writeLock().tryLock()) {
            try {
                if (!closed && (null == holder || isExpired())) {
                    holder = nodeId;
                    expireMs = Instant.now().plusMillis(leaseDurationMs).toEpochMilli();
                    System.out.printf("LeaseManager tryAcquire node %s%n", nodeId);
                    return true;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return false;
    }

    // Extends expiry if nodeId is the current holder and lease hasn't expired.
    // Returns true if renewed. Returns false if closed or expired or wrong holder.
    public boolean renew(String nodeId) {
        if (lock.writeLock().tryLock()) {
            try {
                if (!closed && isLeader(nodeId)) {
                    expireMs = Instant.now().plusMillis(leaseDurationMs).toEpochMilli();
                    System.out.printf("LeaseManager renew node %s%n", nodeId);
                    return true;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        return false;
    }

    // Returns true only if nodeId is the current holder AND lease has not expired AND not closed.
    public boolean isLeader(String nodeId) {
        if (lock.readLock().tryLock()) {
            try {
                return !closed && nodeId.equals(holder) && !isExpired();
            } finally {
                lock.readLock().unlock();
            }
        }
        return false;
    }

    // Simulates the lease manager crashing. All subsequent calls return false.
    public void close() {
        lock.writeLock().lock();
        try {
            closed = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean isExpired() {
        return Instant.ofEpochMilli(expireMs).isBefore(Instant.now());
    }
}
