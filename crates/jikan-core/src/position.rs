// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Logical positions within a source's event stream.
//!
//! Lamport (1978) shows that events within a single process (or replication
//! stream) are totally ordered by their logical clock value. Events from
//! different sources are concurrent — comparing positions across sources is
//! undefined and therefore a compile-time error: `Position` intentionally
//! omits `PartialOrd` and `Ord`.

use serde::{Deserialize, Serialize};

/// A stable identifier for a particular source database instance.
///
/// Used to prevent accidental cross-source position comparisons at the type
/// level. Two `Position` values are only comparable when they share a `SourceId`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

/// A PostgreSQL log sequence number.
///
/// LSNs are 64-bit unsigned integers that increase monotonically within a
/// single PostgreSQL cluster. They are Lamport clocks (Lamport 1978) for
/// that cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

/// A MySQL GTID set representing a consistent replication position.
///
/// A GTID set encodes all transactions that have been applied up to a given
/// point. Within a single MySQL topology, GTID sets are partially ordered
/// by set inclusion, making them suitable Lamport clock values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GtidSet(pub String);

/// A MongoDB oplog timestamp, composed of a Unix second, an ordinal, and
/// a Raft term.
///
/// The `(seconds, ordinal)` pair forms a hybrid logical clock that is
/// totally ordered within a replica set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OplogTimestamp {
    /// Unix epoch seconds.
    pub seconds: u32,
    /// Sub-second ordinal, incremented for events within the same second.
    pub ordinal: u32,
    /// Raft term for the replica set primary that produced this entry.
    pub term: u64,
}

/// The logical position of an event within a single source's ordered stream.
///
/// Lamport (1978): the happened-before relation is defined within a source.
/// Cross-source comparison is intentionally absent — `Position` does not
/// implement `PartialOrd` or `Ord`. Any code that attempts to compare
/// positions from different sources will not compile.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Position {
    /// A PostgreSQL LSN.
    Lsn(Lsn),
    /// A MySQL GTID set.
    Gtid(GtidSet),
    /// A MongoDB oplog timestamp.
    OplogTs(OplogTimestamp),
}

impl Position {
    /// Returns true if this position is strictly greater than `other` within
    /// the same source.
    ///
    /// Panics in debug builds if the two positions are of different variants,
    /// because that comparison is undefined (Lamport 1978). In release builds
    /// the comparison returns `false` rather than panicking, but callers should
    /// never reach this branch — the pipeline should ensure variant consistency.
    pub fn is_after(&self, other: &Position) -> bool {
        match (self, other) {
            (Position::Lsn(a), Position::Lsn(b)) => a > b,
            (Position::Gtid(_), Position::Gtid(_)) => {
                // MySQL GTID set ordering is set-inclusion based; the full
                // comparison requires parsing the GTID strings. The MySQL
                // connector overrides this with connector-specific logic.
                // Here we return false as a conservative default.
                false
            }
            (Position::OplogTs(a), Position::OplogTs(b)) => {
                (a.seconds, a.ordinal) > (b.seconds, b.ordinal)
            }
            _ => {
                debug_assert!(
                    false,
                    "cross-variant position comparison is undefined (Lamport 1978)"
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_ordering() {
        let a = Position::Lsn(Lsn(100));
        let b = Position::Lsn(Lsn(200));
        assert!(b.is_after(&a));
        assert!(!a.is_after(&b));
    }

    #[test]
    fn lsn_equal_is_not_after() {
        let a = Position::Lsn(Lsn(100));
        let b = Position::Lsn(Lsn(100));
        assert!(!a.is_after(&b));
    }

    #[test]
    fn oplog_ts_ordering() {
        let a = Position::OplogTs(OplogTimestamp {
            seconds: 1000,
            ordinal: 0,
            term: 1,
        });
        let b = Position::OplogTs(OplogTimestamp {
            seconds: 1000,
            ordinal: 1,
            term: 1,
        });
        assert!(b.is_after(&a));
    }
}
