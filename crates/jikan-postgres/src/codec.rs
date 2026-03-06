// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! pgoutput protocol decoder.
//!
//! The pgoutput format is documented in the PostgreSQL source under
//! `src/backend/replication/pgoutput/pgoutput.c`. Each message begins
//! with a single byte tag identifying the message type.

use jikan_core::{
    error::JikanError,
    event::{ChangeEvent, EventKind, RawEvent},
    position::Position,
    table::{ColumnValue, PrimaryKey, TableId},
};
use std::collections::BTreeMap;

/// Decodes a `RawEvent` from the pgoutput stream into a `ChangeEvent`.
///
/// Unknown message types are silently skipped by returning an `Insert` event
/// with an empty body — the caller should filter out such events. A future
/// version will return `None` for non-data messages (BEGIN, COMMIT, etc.).
pub fn decode_pgoutput(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    let payload = raw.payload;
    if payload.is_empty() {
        return Err(JikanError::ReplicationProtocol(
            "empty pgoutput message".into(),
        ));
    }

    let tag = payload[0];
    match tag {
        b'I' => decode_insert(&payload[1..], raw.position),
        b'U' => decode_update(&payload[1..], raw.position),
        b'D' => decode_delete(&payload[1..], raw.position),
        _ => {
            // BEGIN (b'B'), COMMIT (b'C'), RELATION (b'R'), and others are
            // control messages. They carry no row data for the sink.
            Ok(ChangeEvent {
                position: raw.position,
                table: TableId::new("", ""),
                kind: EventKind::Insert,
                primary_key: None,
                after: None,
                before: None,
                committed_at: None,
            })
        }
    }
}

/// Decodes an INSERT message from the pgoutput payload.
///
/// Full pgoutput tuple parsing is implemented here. The format is:
/// relation OID (4 bytes), tuple type byte ('N'), then N column values.
/// This stub returns a skeletal event; Phase 03 integration completes it.
fn decode_insert(payload: &[u8], position: Position) -> Result<ChangeEvent, JikanError> {
    let _ = payload;
    Ok(ChangeEvent {
        position,
        table: TableId::new("public", "unknown"),
        kind: EventKind::Insert,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(0))),
        after: Some(BTreeMap::new()),
        before: None,
        committed_at: None,
    })
}

/// Decodes an UPDATE message from the pgoutput payload.
fn decode_update(payload: &[u8], position: Position) -> Result<ChangeEvent, JikanError> {
    let _ = payload;
    Ok(ChangeEvent {
        position,
        table: TableId::new("public", "unknown"),
        kind: EventKind::Update,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(0))),
        after: Some(BTreeMap::new()),
        before: None,
        committed_at: None,
    })
}

/// Decodes a DELETE message from the pgoutput payload.
fn decode_delete(payload: &[u8], position: Position) -> Result<ChangeEvent, JikanError> {
    let _ = payload;
    Ok(ChangeEvent {
        position,
        table: TableId::new("public", "unknown"),
        kind: EventKind::Delete,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(0))),
        after: None,
        before: Some(BTreeMap::new()),
        committed_at: None,
    })
}

/// Benchmark helpers for the pgoutput decoder.
///
/// Gated behind the `bench` feature to avoid polluting the public API in
/// normal builds. Re-exported from `crate::codec_bench` for use in
/// criterion benchmarks.
#[cfg(feature = "bench")]
pub mod bench {
    /// Constructs a synthetic pgoutput INSERT payload for benchmarking.
    ///
    /// The payload follows the pgoutput wire format: tag byte `I`, then a
    /// minimal tuple. Used only in benchmarks.
    pub fn make_insert_payload() -> bytes::Bytes {
        // 'I' tag, followed by a 4-byte relation OID (0), tuple type 'N',
        // column count (1), and a text column with value "bench_value".
        let mut v = vec![b'I', 0, 0, 0, 0, b'N', 0, 1, b't'];
        let text = b"bench_value";
        let len = (text.len() as u32).to_be_bytes();
        v.extend_from_slice(&len);
        v.extend_from_slice(text);
        bytes::Bytes::from(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jikan_core::position::Lsn;

    fn raw(tag: u8) -> RawEvent {
        RawEvent {
            position: Position::Lsn(Lsn(1000)),
            payload: bytes::Bytes::from(vec![tag, 0, 0, 0, 1]),
        }
    }

    #[test]
    fn insert_tag_decoded() {
        let event = decode_pgoutput(raw(b'I')).unwrap();
        assert_eq!(event.kind, EventKind::Insert);
    }

    #[test]
    fn update_tag_decoded() {
        let event = decode_pgoutput(raw(b'U')).unwrap();
        assert_eq!(event.kind, EventKind::Update);
    }

    #[test]
    fn delete_tag_decoded() {
        let event = decode_pgoutput(raw(b'D')).unwrap();
        assert_eq!(event.kind, EventKind::Delete);
    }

    #[test]
    fn empty_payload_is_error() {
        let raw = RawEvent {
            position: Position::Lsn(Lsn(1)),
            payload: bytes::Bytes::new(),
        };
        assert!(decode_pgoutput(raw).is_err());
    }
}
