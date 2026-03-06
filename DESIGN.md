# Jikan - Design Document

This document describes the core design decisions behind Jikan and the theoretical grounding for each. It is written before the implementation is complete, intentionally. Design decisions made explicit before code is written are easier to reason about and harder to violate accidentally.

Every major design decision in this document is traceable to a specific result in the distributed systems literature. Where a decision is a theorem, it is marked as such. Where it is a choice, the alternatives and tradeoffs are stated.

---

## Core Invariants

These are the properties Jikan must maintain under all conditions. They are not aspirational - they are the specification. Any implementation that violates any of these is incorrect.

1. **Monotonicity** - the position cursor only ever moves forward, never backward
2. **No gap** - every event with position ≤ cursor has been delivered to the sink
3. **No duplicate** - no event with position ≤ cursor will be delivered again after a resume
4. **Causality** - if event A happened-before event B (A's log position < B's), A is always delivered before B
5. **Channel state consistency** - in-flight events at the moment of checkpointing are accounted for: either captured in the snapshot or present at the head of the stream on resume
6. **Merge boundary integrity** - the snapshot owns all events at positions ≤ snapshot_lsn; the stream owns all events at positions > snapshot_lsn; this boundary is never violated

---

## Theoretical Foundation

Jikan is built on six papers. This section states the key results from each and maps them to concrete design decisions.

### Lamport (1978) - Logical Clocks and Causal Ordering

**Key result:** In a distributed system with no shared global clock, causal ordering between events can be derived from logical clocks - monotonically increasing counters attached to every event. If event A happened-before event B, A's logical clock value is strictly less than B's. Events with no happened-before relationship are concurrent; no ordering between them should be assumed or derived.

**Application to Jikan:** LSNs (Postgres), GTIDs (MySQL), and oplog timestamps (MongoDB) are Lamport clocks. This is not an analogy - they are the same construct applied at the database level. The happened-before relation between changes within a single source is derived entirely from log position.

**Theorems this establishes:**
- Wall clock time must never be used for event ordering. A change with LSN 1000 happened-before LSN 1001 regardless of what the system clock read at either moment.
- Events from different sources have no happened-before relationship. Cross-source ordering is formally impossible without external coordination. This is a theorem, not a limitation.
- The Position type is a Lamport clock. Its only required property is monotonicity within a source. Positions must never be compared across sources.

### Chandy & Lamport (1985) - Distributed Snapshots

**Key result:** A consistent global state of a distributed system can be recorded without stopping the system and without a global clock, provided: (1) a marker is injected into every channel at the moment recording begins, (2) each process records its local state when it first receives a marker, and (3) each channel records the messages that arrive after the marker is sent but before the marker is received. The resulting snapshot corresponds to a state the system could have passed through, even if no single instant existed where all processes were simultaneously in that state.

**Critical detail the paper emphasizes:** Many published algorithms for recording global state are incorrect because they fail to account for channel state - messages in-flight at the moment of the snapshot. A snapshot that captures process state but ignores in-flight messages produces a state that never actually existed.

**Application to Jikan:** The `snapshot_lsn` is the Chandy-Lamport marker. The stream buffer is the channel state recorder. The merge is the algorithm for combining process state (snapshot chunks) with channel state (buffered stream events).

**Theorems this establishes:**
- Streaming must begin before snapshotting. If Jikan starts the snapshot before opening the stream, there is a window where committed changes (in-flight channel messages) are neither in the snapshot nor in the stream. This is the channel state bug. It is not a performance concern - it is a correctness requirement.
- The snapshot is causally consistent by construction. It corresponds to a state the database could have been in at `snapshot_lsn`, even though the chunks are read sequentially over time.
- Crash resumption is theoretically grounded. The chunk cursor is persistent channel state. Resuming from the last committed cursor is exactly what Chandy-Lamport's fault tolerance analysis prescribes.

### Berenson et al. (1995) - A Critique of ANSI SQL Isolation Levels

**Key result:** The ANSI SQL isolation level definitions are ambiguous and incomplete. They fail to define several anomalies that real implementations can exhibit, most importantly read skew (T1 reads row A, T2 updates rows A and B, T1 reads row B - T1 sees an inconsistent view) and write skew (T1 and T2 each read a consistent state and write based on it, but the combined result violates an invariant). Snapshot isolation, which many databases implement, prevents read skew but not write skew and is therefore not serializable.

**Application to Jikan:** The isolation level of the snapshot transaction determines the consistency of what Jikan reads. The specific anomaly Jikan must prevent is read skew across chunks - if Jikan closes a transaction after each chunk and opens a new one for the next, concurrent writes between chunks produce a snapshot where rows reflect different points in time. This is internally inconsistent and violates invariant 6.

**Theorems this establishes:**
- Snapshot transactions must use REPEATABLE READ minimum. READ COMMITTED permits read skew across chunks and is not safe.
- For parallel chunk reads, a single snapshot must be exported and shared across all reader connections. Opening independent transactions per reader, even at REPEATABLE READ, can produce read skew if their snapshot times differ.
- The name a database gives its isolation level is not reliable. `consistency_model()` documents actual behavior, not the isolation level label.

### Adya, Liskov, O'Neil (2000) - Generalized Isolation Level Definitions

**Key result:** Isolation levels can be defined precisely and implementation-independently using dependency graphs over transactions. A history is serializable if and only if its dependency graph has no cycles. Every isolation anomaly corresponds to a specific cycle pattern. Snapshot isolation sits strictly between repeatable read and serializable - it prevents read skew but allows write skew cycles. A read-only transaction under snapshot isolation is equivalent to serializable because write skew cycles require writes.

**Application to Jikan:** Jikan's snapshot transaction is read-only. A read-only transaction under snapshot isolation has no write-depends or anti-depends edges that could form a write skew cycle. Therefore Jikan's snapshot transaction under REPEATABLE READ is PL-3 equivalent - provably correct, not just "probably fine."

**The merge boundary as anti-dependency prohibition:** Delivering a snapshot version of a row after a stream version for the same row at a higher log position creates an anti-dependency cycle in Adya's graph. The consumer sees a version that happened-before the one they already saw. This is a consistency anomaly by formal definition. Invariant 6 is the prohibition on this cycle.

**Application to `consistency_model()`:** The return values of this method should be grounded in Adya's PL levels, not informal descriptions. Each source implementation documents which PL level its snapshot and stream guarantees correspond to.

### Carbone et al. (2015) - Lightweight Asynchronous Snapshots (Flink ABS)

**Key result:** Chandy-Lamport distributed snapshots can be adapted for streaming dataflows by injecting barriers into data streams. Barriers divide the stream into records belonging to checkpoint N and records belonging to checkpoint N+1. When an operator has received barriers on all input channels, it snapshots its local state and forwards the barrier. The resulting checkpoint is consistent and complete. Exactly-once delivery is guaranteed if: (1) channels are FIFO, (2) barriers are injected at a consistent position, (3) operator state is snapshotted atomically on barrier receipt, and (4) recovery replays from the checkpointed position.

**Critical failure mode identified:** If the barrier position is not durable before processing begins, a crash causes reprocessing of pre-barrier records as if they hadn't been processed, producing gaps or duplicates depending on merge behavior.

**Application to Jikan:** All four exactly-once conditions hold:
1. WAL, binlog, and oplog are FIFO by definition
2. `snapshot_lsn` / `snapshot_gtid_set` / `snapshot_ts` is the barrier, fixed before any work begins
3. Chunk cursor is snapshotted atomically after each chunk, persisted to disk before the next chunk begins
4. On crash, stream replays from last checkpointed position via replication slot / GTID set / resume token

**Hard requirement from the failure mode analysis:** `snapshot_lsn` must be persisted to disk before any snapshot chunk is read. An in-memory-only barrier that is lost on crash violates the exactly-once proof.

**Sink coordination (from Flink State 2017 and Flink Blog 2018):** End-to-end exactly-once requires sink participation in the checkpoint protocol via two-phase commit. Sinks pre-commit on barrier receipt, commit on checkpoint completion, abort on recovery. Sinks that do not support 2PC provide at-least-once semantics. This is documented honestly per sink.

**Aligned vs unaligned checkpoints:** Aligned (do not advance stream past `snapshot_lsn` until snapshot completes) is simpler and correct. Unaligned (buffer stream events past `snapshot_lsn` during snapshot, include buffer in checkpoint state) is an optimization for high-throughput scenarios. Jikan v1 uses aligned. Unaligned is a documented future optimization.

### Lloyd et al. (2011) - COPS and Causal Consistency

**Key result:** Causal consistency - if A happened-before B, every observer that has seen B has already seen A - is the strongest consistency model achievable while maintaining availability under partition. It is strictly stronger than eventual consistency and strictly weaker than sequential consistency. Systems providing only eventual consistency are giving up ordering they could preserve for free.

**Application to Jikan:** Within a single source, Jikan provides total causal order - a total order that is consistent with causal ordering, where every pair of events has a defined happened-before relationship derived from log position. This is stronger than causal consistency. Across sources, Jikan provides eventual consistency only. This is not a choice - it follows from Lamport (1978). Events from independent sources are concurrent; no causal ordering exists between them.

**Monotonic reads and read-your-writes:** COPS identifies these as properties that causal consistency provides. Both hold automatically in Jikan. LSN ordering guarantees monotonic reads - a consumer never sees an older version after a newer one. Replication slot ordering guarantees read-your-writes - a committed write has a higher LSN than the write itself and is always delivered after.

**Sink consistency:** Jikan's consistency guarantee is only as strong as the weakest link. A Kafka sink with a single partition preserves causal order. A webhook sink with unordered HTTP delivery provides eventual consistency to the endpoint regardless of what Jikan delivers. This is documented per sink.

---

## Ordering Model

Event ordering is derived from log position, not wall clock time. This is a theorem (Lamport 1978), not a design preference.

Each source exposes a monotonically increasing position marker:
- **PostgreSQL** - Log Sequence Number (LSN): a monotonically increasing byte offset into the WAL stream
- **MySQL / MariaDB** - Global Transaction ID (GTID): `server_uuid:transaction_id`, monotonically increasing transaction_id per server
- **MongoDB** - oplog Timestamp + ordinal: `(seconds, ordinal)` BSON Timestamp, monotonically increasing within a replica set

These are Lamport clocks. Jikan's Position type wraps them:

```rust
enum Position {
    Lsn(u64),                          // Postgres
    Gtid(GtidSet),                     // MySQL: set of server_uuid:transaction_id
    OplogTs { secs: u32, ordinal: u32, term: u64 },  // MongoDB
}
```

Positions are comparable within a source. They are never compared across sources. The compiler should enforce this - cross-source Position comparison is a type error, not a runtime check.

---

## Snapshot Design

### The Algorithm

Jikan's snapshot algorithm is a direct application of Chandy-Lamport (1985) adapted via Carbone (2015):

```
Step 1: Record snapshot_lsn (the marker) - persist to disk immediately
Step 2: Begin streaming changes from snapshot_lsn - open the channel recorder
Step 3: Snapshot table in chunks with a cursor (primary key range)
Step 4: After each chunk, persist cursor state to disk (checkpoint)
Step 5: Merge snapshot chunks with buffered stream events by log position
Step 6: Drain stream buffer in order once all chunks complete
Step 7: Transition to pure streaming from the current position
```

Step 1 must complete before Step 2. Step 2 must complete before Step 3. These orderings are correctness requirements, not performance suggestions.

### The Merge Boundary

The merge boundary is the central correctness invariant (Invariant 6):

- Events at positions ≤ `snapshot_lsn`: delivered via snapshot. If a stream event exists for the same row at position ≤ `snapshot_lsn`, the snapshot version supersedes it.
- Events at positions > `snapshot_lsn`: delivered via stream. If a snapshot row has a stream event at position > `snapshot_lsn`, the stream version supersedes it.

Violating this boundary creates an anti-dependency cycle in Adya's dependency graph. It is a consistency anomaly by formal definition.

### Snapshot Transaction Isolation

Snapshot transactions use REPEATABLE READ minimum (Berenson 1995). READ COMMITTED is not acceptable - it permits read skew across chunks, producing a snapshot that reflects multiple points in time simultaneously.

Per source:
- **Postgres**: `BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ` - true snapshot isolation from transaction start
- **MySQL**: `START TRANSACTION WITH CONSISTENT SNAPSHOT` - snapshot isolation at the current binlog position, returns the GTID set at that moment
- **MongoDB**: `snapshot` read concern with `atClusterTime: snapshot_ts` in a session transaction

For parallel chunk reads, the snapshot must be shared across reader connections:
- **Postgres**: `pg_export_snapshot()` exports a snapshot ID; worker connections import it via `SET TRANSACTION SNAPSHOT`. All workers see the same consistent point in time.
- **MySQL**: all workers share the same `START TRANSACTION WITH CONSISTENT SNAPSHOT` session, or use exported snapshot via `BINLOG_SNAPSHOT_*` variables.
- **MongoDB**: all workers use the same session with the same `atClusterTime`.

### Chunk Cursor

The chunk cursor is the persistent Chandy-Lamport channel state. It records:

```rust
struct ChunkCursor {
    table: TableId,
    last_pk: Option<PrimaryKey>,   // last primary key seen in current chunk
    snapshot_position: Position,   // the snapshot_lsn - never changes during a snapshot
    chunk_index: u64,              // monotonically increasing chunk counter
}
```

This is persisted to disk after every chunk, before the next chunk begins. A crash at any point - mid-chunk, between chunks, during the merge - resumes from the last committed cursor. The snapshot position never changes on resume; only `last_pk` and `chunk_index` advance.

### Crash Recovery

On restart after a crash:
1. Load last committed `ChunkCursor` from disk
2. Re-open the stream from `snapshot_position` (replication slot / GTID set / resume token guarantees this is available)
3. Resume reading chunks from `last_pk`
4. Continue merge from where it left off

No data is reread that has already been delivered to the sink. No data is missed. This is the Chandy-Lamport fault tolerance guarantee applied to CDC.

---

## Consistency Model

### Per-Source Guarantees

```rust
enum ConsistencyGuarantee {
    /// Total causal order within this source.
    /// Every pair of events has a defined happened-before relationship.
    /// If A happened-before B, A is always delivered before B.
    /// Stronger than causal+. Equivalent to causal consistency with total order.
    TotalCausalOrder,

    /// Causal consistency within this source, with documented caveats.
    CausalWithCaveats(Vec<ConsistencyCaveat>),

    /// No ordering guarantee across sources.
    /// Concurrent events from independent sources have no defined order.
    /// This is a theorem (Lamport 1978), not a limitation.
    EventualAcrossSources,
}

enum ConsistencyCaveat {
    /// MongoDB sharded cluster: cross-shard concurrent events have no total order.
    /// Causally related events are ordered. Concurrent cross-shard events are not.
    ShardedClusterConcurrentOrdering,
}
```

Per source:
- **Postgres**: `TotalCausalOrder` - LSN is a total order over all changes
- **MySQL**: `TotalCausalOrder` - GTID transaction_id is a total order within a server
- **MongoDB (single replica set)**: `TotalCausalOrder` - oplog `(ts, ordinal, term)` is a total order
- **MongoDB (sharded cluster)**: `CausalWithCaveats([ShardedClusterConcurrentOrdering])` - causally related events ordered, concurrent cross-shard events not

Cross-source: `EventualAcrossSources`. Not documented as a limitation. Documented as a theorem.

### Automatic Properties

These hold for all single-source streams without additional implementation effort:

**Monotonic reads** - a consumer never sees an older version of a row after seeing a newer one. Guaranteed by LSN ordering.

**Read-your-writes** - a write that commits at position P is always delivered after all writes at positions < P. Guaranteed by replication slot ordering.

### Sink Consistency

Jikan's output consistency guarantee is bounded by the sink:

| Sink | End-consumer consistency |
|------|-------------------------|
| Postgres / MySQL (ordered writes) | TotalCausalOrder preserved |
| Kafka (single partition per table) | TotalCausalOrder preserved |
| Kafka (multiple partitions) | Causal within partition, eventual across |
| gRPC (ordered stream) | TotalCausalOrder preserved |
| Webhook / HTTP | Eventual - HTTP delivery order not guaranteed |
| File / stdout | TotalCausalOrder preserved for sequential readers |

---

## Source Trait

Every source implements a common trait. The `consistency_model()` method is not cosmetic - it documents actual isolation semantics per source, grounded in Adya (2000).

```rust
trait Source {
    /// Current position in the source log.
    async fn position(&self) -> Position;

    /// Stream of raw events starting from pos.
    /// Events are delivered in log position order.
    /// FIFO guarantee required for Carbone (2015) exactly-once proof.
    async fn stream_from(&self, pos: Position) -> Stream<RawEvent>;

    /// Read one chunk of a table snapshot starting after cursor.
    /// Transaction isolation level must be REPEATABLE READ minimum (Berenson 1995).
    async fn snapshot_chunk(&self, table: &str, cursor: ChunkCursor) -> Chunk;

    /// Export snapshot for sharing across parallel readers.
    /// Returns an opaque snapshot handle that worker connections can import.
    async fn export_snapshot(&self) -> SnapshotHandle;

    /// Import a previously exported snapshot.
    /// All workers sharing a handle see the same consistent point in time (Berenson 1995).
    async fn import_snapshot(&self, handle: SnapshotHandle) -> ();

    /// Consistency guarantees this source can provide.
    /// Grounded in Adya (2000) PL levels.
    fn consistency_model(&self) -> ConsistencyGuarantee;

    /// WAL / binlog / oplog health metrics.
    async fn lag_metrics(&self) -> LagMetrics;
}
```

---

## Sink Trait

Sinks that support exactly-once implement the full 2PC protocol (Carbone 2015, Flink Blog 2018). Sinks that do not support transactions implement at-least-once only.

```rust
trait Sink {
    /// Deliver a batch of events.
    async fn write(&mut self, events: Vec<Event>) -> Result<()>;

    /// Pre-commit: write to staging area, do not make visible.
    /// Called when a checkpoint barrier is received.
    async fn pre_commit(&mut self, checkpoint_id: u64) -> Result<()>;

    /// Commit: make pre-committed data visible atomically.
    /// Called when checkpoint is confirmed complete.
    async fn commit(&mut self, checkpoint_id: u64) -> Result<()>;

    /// Abort: discard pre-committed data.
    /// Called on recovery if pre-commit exists but commit does not.
    async fn abort(&mut self, checkpoint_id: u64) -> Result<()>;

    /// Whether this sink supports exactly-once via 2PC.
    fn exactly_once_capable(&self) -> bool;
}
```

Per-sink exactly-once capability:

| Sink | Exactly-once | Mechanism |
|------|-------------|-----------|
| Postgres | Yes | Transaction per checkpoint |
| MySQL | Yes | Transaction per checkpoint |
| Kafka | Yes | Kafka transactional producer |
| gRPC | No | At-least-once |
| Webhook | No | At-least-once |
| File | No | At-least-once (idempotent writes possible) |

---

## Database Safety

### The WAL Retention Problem

Postgres retains WAL indefinitely for any replication slot that is not advancing. MySQL retains binlog until `binlog_expire_logs_seconds`. MongoDB's oplog is a capped collection that rolls over continuously. In all cases, if Jikan stops consuming, the source accumulates backlog until something breaks.

This is not a theoretical concern. It is a documented production failure mode.

### WAL Watchdog

The watchdog is a first-class component, not an afterthought. It monitors:

**Postgres:**
- `pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)` - WAL lag in bytes
- `wal_status` from `pg_replication_slots` - `reserved` / `extended` / `unreserved` / `lost`
- Alert at configurable warn threshold (default 1GB)
- Alert loudly at hard threshold (default 5GB)
- On `wal_status = lost`: critical alert, initiate recovery (new slot, fresh snapshot)

**MySQL:**
- Distance between Jikan's current GTID position and the oldest available binlog file
- Time elapsed since Jikan last advanced its position
- Alert when lag approaches `binlog_expire_logs_seconds` boundary
- On position invalidation: critical alert, initiate recovery (fresh snapshot)

**MongoDB:**
- Oplog window: `latest_ts - earliest_ts` in the `local.oplog.rs` collection
- Jikan's processing lag vs the oplog window
- Alert when lag exceeds configurable fraction of oplog window (default 50%)
- On `ChangeStreamHistoryLost` error: critical alert, initiate recovery (fresh snapshot)

**Jikan will never be the reason your database goes down.**

### `max_slot_wal_keep_size` Tradeoff

Postgres 13+ supports `max_slot_wal_keep_size`. Setting it prevents disk exhaustion but introduces slot invalidation risk if Jikan is down longer than the threshold allows. Not setting it prevents slot invalidation but risks disk exhaustion.

Jikan documents this tradeoff explicitly and recommends setting `max_slot_wal_keep_size` with a value large enough to survive expected Jikan downtime windows, paired with the WAL watchdog alerting well before the threshold is approached.

---

## Source Prerequisites

### Postgres

Required server configuration:
- `wal_level = logical` (requires server restart)
- `max_replication_slots >= 1`

Required per-table configuration:
- `REPLICA IDENTITY FULL` - required for full old-row images on UPDATE and DELETE. Without this, only the primary key is included in change events. Jikan will warn loudly on tables without REPLICA IDENTITY FULL and document what data is missing.

Jikan creates and manages its own replication slot. Slot state is tracked explicitly - Jikan knows the LSN, `wal_status`, and lag of every slot it owns.

### MySQL

Required server configuration:
- `binlog_format = ROW` - STATEMENT and MIXED cannot reliably reconstruct row changes
- `binlog_row_image = FULL` - MINIMAL only logs changed columns; Jikan needs full row images
- `gtid_mode = ON` - GTIDs required for stable position tracking across topology changes
- `enforce_gtid_consistency = ON`

Required Jikan configuration:
- Stable `server_id` - unique 32-bit integer per Jikan deployment, persisted to disk. Must not be shared with any other replica in the topology. Random-per-startup is not acceptable.

### MongoDB

Required server configuration:
- Replica set mode - change streams require a replica set. Standalone MongoDB does not support change streams.

Required per-collection configuration (for full before-change images):
- `changeStreamPreAndPostImages: true` - required for `fullDocumentBeforeChange`. Without this, UPDATE and DELETE events do not include the pre-change document.

---

## Schema Changes

DDL changes are WAL events like any other. Handling them correctly is hard. Jikan v1 takes a conservative approach.

**Postgres:** Schema changes arrive as Relation messages in the pgoutput stream. Jikan detects schema changes, applies pending events under the old schema up to the DDL LSN, then switches to the new schema. Column identity is tracked by OID, not name - a column renamed and re-added with the same name but different type is detected correctly.

**MySQL:** Schema changes arrive as `QUERY_EVENT` entries containing the SQL text. Jikan detects DDL statements (`ALTER TABLE`, `CREATE TABLE`, `DROP TABLE`, `RENAME TABLE`), logs them with the GTID position, and surfaces them to the operator.

**MongoDB:** Collection-level changes arrive as `c` (command) entries in the change stream. `dropCollection`, `renameCollection`, and `createIndexes` are detected and logged.

**v1 behavior for all sources:** Detect schema changes, emit a structured event describing the change, log loudly, and surface to the operator. Automatic schema evolution (propagating DDL to the sink) is a v2 feature. v1 does not silently corrupt - it detects and alerts.

---

## Pgoutput Protocol Versions

Jikan targets pgoutput v1 (Postgres 10+) as the initial implementation. This covers the vast majority of Postgres installations.

**v1 limitation:** Large transactions are buffered entirely in memory on the Postgres server before being sent to Jikan. A very large transaction means significant memory pressure before Jikan sees a single byte.

**v2 (Postgres 14+):** Adds in-progress transaction streaming. Large transactions are streamed incrementally as they are written to WAL. This is a documented upgrade path for operators hitting the v1 memory limitation.

**v3 (Postgres 15+):** Adds support for prepared transactions (2PC at the source database level). Required only for sources using distributed transactions.

---

## What Jikan Is Not

- Not a message queue
- Not a data warehouse
- Not a managed service
- Not a framework
- Not an ETL pipeline

Jikan is a single binary that moves changes from a source database to a destination, correctly, safely, and without operational overhead. Every design decision is in service of that and only that.

---

## Theoretical References

All design decisions in this document are traceable to one of the following:

| Paper | Year | What it establishes in Jikan |
|-------|------|------------------------------|
| Lamport - Time, Clocks, and the Ordering of Events | 1978 | LSNs as Lamport clocks; causal ordering; cross-source ordering impossibility |
| Chandy & Lamport - Distributed Snapshots | 1985 | Snapshot algorithm; stream-before-snapshot requirement; chunk cursor as channel state; crash resumption |
| Berenson et al. - A Critique of ANSI SQL Isolation Levels | 1995 | REPEATABLE READ requirement; read skew across chunks; snapshot export for parallel reads |
| Adya, Liskov, O'Neil - Generalized Isolation Level Definitions | 2000 | Read-only snapshot = PL-3 equivalent; merge boundary as anti-dependency prohibition; consistency_model() vocabulary |
| Carbone et al. - Lightweight Asynchronous Snapshots | 2015 | ABS algorithm; snapshot_lsn durability requirement; stream buffer as correctness requirement; sink 2PC interface |
| Lloyd et al. - Don't Settle for Eventual (COPS) | 2011 | TotalCausalOrder per source; EventualAcrossSources as theorem; sink consistency bounds; monotonic reads; read-your-writes |

---

*This document describes design intent prior to implementation. It will be updated as the implementation reveals things the design did not anticipate. When it is updated, the theoretical basis for any change will be stated.*
