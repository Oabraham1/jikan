# Jikan

> Change Data Capture that just works.

Built in Rust. Solo project. Love of the game.

---

## What Is Jikan?

Jikan is an open source Change Data Capture (CDC) tool that streams every change from your database - every insert, update, delete - to wherever it needs to go, in real time, reliably, and without operational overhead.

Every company has a primary database - the source of truth - but that data needs to be in multiple places simultaneously:

- Data warehouse for analytics (Snowflake, BigQuery, Redshift)
- Search index for fast queries (Elasticsearch, Typesense)
- Cache for low latency reads (Redis)
- Other microservices that need to react to changes
- Audit log for compliance
- Replica in another region for disaster recovery

Without CDC you solve this with batch jobs, dual writes, and manual ETL pipelines - all of which are slow, brittle, and hard to maintain. Jikan taps directly into your database's change log and propagates changes in milliseconds, with zero application code changes required.

---

## The Problem With Most CDC Tools

CDC tooling has come a long way, but a few hard problems keep showing up in production:

- **Snapshots are fragile** - initial snapshots on large tables can take hours, and a failure midway means starting over from scratch
- **Infrastructure overhead** - many tools require a message broker as a hard dependency just to get CDC working, which means more to operate and more that can go wrong
- **WAL retention is a silent risk** - if a connector goes down and nobody notices, the database keeps accumulating write-ahead log until disk fills up and the database crashes
- **Schema changes are hard** - DDL changes can break pipelines in ways that are difficult to recover from
- **Operational opacity** - it's often hard to know what the tool is doing, how far behind it is, or whether something is quietly going wrong

Jikan is designed to address all of these directly.

---

## Core Design Principles

### 1. Resumable Snapshots

The initial snapshot is the hardest part of CDC. On large tables it can take hours, and if it fails partway through, most tools start over from scratch.

Jikan takes a different approach:

```
[record position immediately] → [stream changes in background] →
[snapshot in chunks with cursor] → [merge lazily] →
[crash? resume from last chunk cursor, not from scratch]
```

Snapshots are broken into chunks with a persistent cursor. Jikan never loads an entire table into memory. If a snapshot is interrupted, it resumes from the last committed chunk - not from the beginning.

This is grounded in the Chandy-Lamport distributed snapshot algorithm (1985), adapted for the CDC use case.

### 2. Database Safety First

Jikan is designed to be a good citizen on your database. WAL lag is a first-class metric, always visible. If replication slot retention grows beyond a configurable threshold, Jikan acts and alerts before it becomes a problem. Cursor state is persisted to disk, not memory, so crashes are safe by design.

**Jikan will never be the reason your database goes down.**

### 3. No Infrastructure Dependencies

Jikan is a single binary. No message broker, no JVM, no cluster required.

```bash
# Install
curl -sf https://jikan.io/install.sh | sh

# Start
jikan start --source postgres://... --sink your-destination
```

Sinks are pluggable - stream to Kafka if you already have it, or directly to a database, webhook, gRPC endpoint, file, or anything else. The tool works without any of them.

### 4. Correct Ordering Semantics

Jikan uses LSNs, GTIDs, and oplog positions as logical clocks - grounded in Lamport's foundational work on the ordering of events in distributed systems (1978). Events are ordered by their position in the log, not by wall clock time, which means ordering is correct regardless of network delays or clock skew between machines.

### 5. Honest Guarantees

The `consistency_model()` method on the Source trait exists for a reason - not every database makes the same guarantees, and Jikan doesn't pretend otherwise. What Jikan can deliver is clearly documented per source.

---

## Supported Sources

**At Launch (Tier 1):**
- PostgreSQL (via pgoutput logical replication)
- MySQL / MariaDB (via binlog, row-based)
- MongoDB (via change streams / oplog)

**Planned (Tier 2):**
- CockroachDB (via rangefeed)
- SQL Server

**Maybe someday:**
- Cassandra
- DynamoDB
- Oracle

---

## Supported Sinks

**At Launch:**
- PostgreSQL / MySQL (write directly to a target database)
- Kafka (for those who already have it)
- gRPC (stream to any gRPC endpoint via protobuf)
- Webhook / HTTP
- File / stdout

**Planned:**
- Elasticsearch / Typesense
- Redis
- S3 / object storage

---

## Architecture (Early Sketch)

```
┌─────────────────────────────────────────────────┐
│                  Jikan Core                      │
│                                                  │
│  ┌─────────────┐    ┌──────────────────────┐    │
│  │  Snapshot   │    │   Stream Merger       │    │
│  │  Engine     │←───│   (Chandy-Lamport     │    │
│  │  (chunked)  │    │    inspired)          │    │
│  └─────────────┘    └──────────────────────┘    │
│                                                  │
│  ┌─────────────────────────────────────────┐    │
│  │         Checkpoint Manager               │    │
│  │   (cursor state, LSN, recovery)          │    │
│  └─────────────────────────────────────────┘    │
│                                                  │
│  ┌─────────────────────────────────────────┐    │
│  │         WAL Watchdog                     │    │
│  │   (lag metrics, slot health, alerts)     │    │
│  └─────────────────────────────────────────┘    │
└───────────────────┬─────────────────────────────┘
                    │  Source trait
         ┌──────────┼──────────┐
         │          │          │
      Postgres    MySQL     MongoDB
     (pgoutput)  (binlog)   (oplog)
```

### The Source Trait (Rust)

```rust
trait Source {
    async fn position(&self) -> Position;
    async fn stream_from(&self, pos: Position) -> Stream<RawEvent>;
    async fn snapshot_chunk(&self, table: &str, cursor: Cursor) -> Chunk;
    fn consistency_model(&self) -> ConsistencyGuarantee;
}
```

---

## Theoretical Foundation

Jikan is built on well-established distributed systems theory. The core problems have been solved - the gap has always been packaging.

Key papers that inform the design:

- **Lamport (1978)** - Time, Clocks, and the Ordering of Events in a Distributed System. The foundation for all logical clock reasoning in Jikan.
- **Chandy & Lamport (1985)** - Distributed Snapshots. The theoretical basis for resumable snapshot design.
- **Berenson et al. (1995)** - A Critique of ANSI SQL Isolation Levels. Why isolation definitions matter for CDC guarantees.
- **Adya, Liskov, O'Neil (2000)** - Generalized Isolation Level Definitions. What "consistent snapshot" actually means mathematically.
- **Carbone et al. (2015)** - Lightweight Asynchronous Snapshots for Distributed Dataflows. How Flink implemented Chandy-Lamport for streaming - directly informs Jikan's merge strategy.
- **Lloyd et al. (2011)** - Don't Settle for Eventual: Scalable Causal Consistency with COPS. The vocabulary for what ordering Jikan can and cannot promise downstream.

---


## Building In Public

Following the entire design and build process publicly:

- **Substack** - [link coming] - paper breakdowns, architecture decisions, build log
- **GitHub** - [link coming]
- **Twitter/X** - [link coming]

---

## Name

**Jikan (時間)** - Japanese for time.

Named after Lamport's foundational work on logical clocks and the ordering of events in distributed systems. Time is the core primitive of CDC - everything is about ordering, position, and the happened-before relationship between events.

---

## Status

Pre-alpha. Reading the papers, designing the architecture, building in public.

---

*Built with Rust. Built alone. Built right.*
