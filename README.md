# tinystore — a distributed key-value store in C++, from scratch

A from-scratch distributed key-value store in C++17. No Redis, no Raft
library, no networking framework — just a binary TCP protocol, an epoll
reactor, a consistent hash ring, and the replication and failure-handling
logic, all written by hand.

**[Live demo →](https://seampm.github.io/tinystore/)** — interactive cluster
visualization: consistent hashing, request routing, replication, and failover.

## What's inside

**Binary protocol** (`src/protocol.h`)
- Length-prefixed frames with CRC-free incremental parsing: handles
  fragmentation and pipelining (multiple in-flight requests per connection)
- Ops: `GET`, `SET` (with TTL), `DEL`, `PING`, `STATS`, plus internal
  `REPL_SET` / `REPL_DEL` / `SYNC`

**Storage engine** (`src/store.h`)
- Sharded in-memory hash map (64 shards, `shared_mutex`), per-key TTL with a
  background sweeper
- Every write carries a `(timestamp, node_id)` version stamp; deletes are
  tombstones — so replicas converge last-writer-wins even when updates arrive
  out of order

**Server** (`src/server.h`)
- Linux epoll reactor with edge-triggered I/O, a worker pool, and eventfd
  wakeups for completed responses
- Per-connection worker affinity: one connection's requests are always handled
  by the same worker, so pipelined responses return in request order

**Clustering** (`src/cluster.h`)
- Consistent hash ring (160 virtual nodes, FNV-1a with an avalanche finalizer
  — plain FNV-1a skewed sequential keys badly, measured before/after)
- Any node accepts any key and routes to the owner; the ring is cached and
  rebuilt only when the live set changes
- Heartbeat-based liveness, dead-node removal, and replica failover
- Replication is **asynchronous by design**: the owner applies a write, acks
  the client, and a background per-peer sender delivers it at-least-once
  (idempotent via version stamps). This keeps request workers from ever
  blocking on the network for replication, which is what keeps the cross-node
  wait graph acyclic — a synchronous design deadlocked under pipelined load
  during development, and the timeout cascade it produced is what motivated
  the change
- A restarted node heals by streaming a `SYNC` snapshot from a live peer

**Failure model** (documented, not hidden): with replication factor R the
cluster serves through R-1 concurrent node failures, since every key lives on
R distinct nodes and the ring skips dead ones. The tradeoff for async
replication: a node that dies inside the replication window (typically
sub-millisecond on LAN) can lose an acknowledged write. Two owners can briefly
disagree during a membership change; version stamps make the outcome
deterministic.

**Tooling** (`src/cli.cpp`, `src/bench.cpp`)
- `tinystore-cli`: interactive shell plus one-shot commands
- `tinystore-bench`: multi-threaded latency/throughput benchmark with
  configurable pipeline depth, connection count, and read/write mix

## Validation

- **48/48 unit checks pass**: protocol framing, fragmented/pipelined parsing,
  version conflicts, tombstones, TTL, concurrent store access, ring
  distribution and minimal remapping
- **253/253 integration checks pass** against real multi-process clusters:
  three-node routing, replication, `kill -9` failover with reads surviving on
  replicas, writes during an outage, restart with `SYNC` healing, replicated
  deletes

## Benchmarks

Measured on a 2-core VM, localhost, 4 server workers, 4 benchmark threads ×
4 connections × pipeline depth 16, 64-byte values, 60k-key space:

| Setup | Throughput | p50 | p99 |
|---|---|---|---|
| Single node, repl=1, 90% GET | 150,169 ops/sec | 0.262 ms | 1.031 ms |
| 3 nodes, repl=2, 90% GET | 31,707 ops/sec | 0.967 ms | 11.43 ms |
| 3 nodes, repl=2, 50% GET | 20,603 ops/sec | 1.458 ms | 18.48 ms |

Cluster runs push all traffic through one node, so roughly two thirds of
requests pay a forwarding hop; replication itself is off the critical path.
No Redis comparison is claimed — Redis wasn't available in the test
environment.

Reproduce: `make && ./build/test_unit && ./build/test_cluster`, then

```
./build/tinystore-server --id 0 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --repl 2 &
./build/tinystore-server --id 1 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --repl 2 &
./build/tinystore-server --id 2 --peers 127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 --repl 2 &
./build/tinystore-bench --node 127.0.0.1:7000 --threads 4 --conns 4 --pipeline 16
```

## Layout

```
src/      protocol, store, ring, epoll server, cluster, CLI, benchmark
tests/    unit suite + multi-process cluster integration suite
docs/     GitHub Pages demo (interactive cluster/failover visualization)
```
