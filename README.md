# Mini Zookeeper

Built from scratch in Java. No external RPC or storage frameworks.
This phase layers fault-tolerance concerns on top of the Phase 1 RPC foundation,
working through the problem one step at a time.

---

## What was built

### RPC Layer 

| Class | What it does |
|---|---|
| `MessageFramer` | 4-byte big-endian length prefix framing over raw TCP streams |
| `RpcSerializer` | Binary encode/decode for `RpcRequest` / `RpcResponse` |
| `RpcServer` | One virtual thread per client connection, persistent loop, server-side idempotency |
| `RpcClient` | Async pipelined client — reader thread, `CompletableFuture` pending map, lazy connection |
| `IdempotentCache<T>` | TTL-aware exactly-once cache keyed by request ID |
| `RetryableException` | Typed signal for server-side retry with exponential backoff + jitter |

### Key-Value Store

| Class | What it does |
|---|---|
| `KvServer` | In-memory `ConcurrentHashMap` store with `put`, `get`, `delete` handlers registered on `RpcServer` |
| `SimpleKvClient` | Synchronous typed wrapper around `RpcClient` |
| `KvClient` | Interface — `put`, `get`, `delete`, `close` |

### Replication

| Class | What it does |
|---|---|
| `PrimaryKvServer` | Synchronous write replication — writes to local store then to all replicas before replying |
| `FailoverKvClient` | Client-side failover — tries primary, on failure increments to next endpoint and retries |

### Failure Detection

| Class | What it does |
|---|---|
| `NodeStatus` | `HEALTHY` / `SUSPECTED` / `DOWN` enum |
| `HealthMonitor` | Tracks last-heartbeat timestamps; marks nodes `SUSPECTED` then `DOWN` after missed beats |
| `HeartbeatSender` | Sends periodic heartbeats from a node to a `HealthMonitor` |

### Lease-Based Leadership

| Class | What it does |
|---|---|
| `LeaseManager` | TTL lease with `tryAcquire`, `renew`, `isLeader`, `close` — simulates a central coordinator |
| `LeasedPrimaryKvServer` | Like `PrimaryKvServer` but rejects writes with `NOT_LEADER` if lease is not held |

---

## Key concepts learned

### Exactly-once semantics

At-least-once delivery (client retries on timeout) + server-side idempotency cache = exactly-once execution.
`ConcurrentHashMap.compute()` must be used instead of `computeIfAbsent()` to enforce TTL on every access.

### Lazy connection and the partially-constructed object trap

`new Socket()` is non-null and non-closed before `connect()` is called.
A double-checked locking guard (`isSocketConnected()`) must also check `socket.isConnected()`.
The socket should be assigned to the shared field only *after* `connect()` succeeds — use a local variable during setup.

### TCP stream framing

TCP is a byte stream, not a message protocol.
Without explicit length prefixes, the receiver cannot tell where one message ends and the next begins.
`SocketTimeoutException` on a mid-read timeout corrupts the stream — use `CompletableFuture.orTimeout()` on the caller side instead of `setSoTimeout()` on the socket.

### Synchronous vs asynchronous replication

Synchronous: primary waits for all replicas before replying. Strong consistency, higher latency.
Asynchronous: primary replies immediately, replicates in the background. Lower latency, risk of data loss on crash.
Async replication needs a write-ahead log (WAL) and replication log with sequence numbers to recover safely — deferred to Phase 3.

### Heartbeat-based failure detection

You cannot distinguish a crashed node from a slow one.
`HealthMonitor` escalates: `HEALTHY → SUSPECTED → DOWN` based on missed heartbeat windows.
False positives (healthy node suspected) are unavoidable — the system can only suspect, not confirm.

### Split-brain

Manual failover is dangerous: if the old primary is slow (not dead), promoting a replica creates two active primaries.
Both accept writes for the same keys. Data diverges with no way to reconcile without external coordination.

### Lease-based leadership

A TTL lease prevents split-brain: only the lease holder accepts writes.
The lease must be renewed before expiry — a paused or partitioned primary's lease expires naturally.
**The unsolved problem**: the lease manager itself is a single point of failure.
To replicate the lease manager requires the nodes to *agree* on who holds it.
That agreement is consensus — which is what (Raft) implements.

### CAP theorem in practice

With a lease: during a network partition, the old primary's lease expires → it stops accepting writes (unavailable).
The new primary acquires the lease → it becomes available.
The system chose *consistency over availability* during the partition window.
This is the CP trade-off. Eventual-consistency systems make the opposite choice (AP).

### Consensus (Raft)

*Leader election*: RaftNode(term, vote and state), election timeouts
*Log replication*: leader maintains an append only log of commands and replicates to followers, entries are committed after majority votes
*Network partitioning*: minorty partition supports reads but fails to write, while majority proceeds, heal partition as minority nodes catches up via log replication
*Snapshotting*: as log grows continuously - implement snapshots, install snapshot RPC to sync lagging followers

### Coordination Service

Coordination service built on top of Raft layer
ZNodeTree - hierarchical key-value namespace
Watches - client registers a watch on a znode and gets notified of change
Ephemeral Nodes - znode deleted automaticalls when client session ends

---
