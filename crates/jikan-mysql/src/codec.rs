// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! MySQL binlog event decoder.
//!
//! The binlog format is documented in the MySQL internals manual at
//! <https://dev.mysql.com/doc/internals/en/binary-log.html>. The key
//! event types for CDC are WRITE_ROWS_EVENT (insert), UPDATE_ROWS_EVENT,
//! DELETE_ROWS_EVENT, and QUERY_EVENT (DDL).

use jikan_core::{
    error::JikanError,
    event::{ChangeEvent, EventKind, RawEvent},
    table::{ColumnValue, PrimaryKey, TableId},
};
use std::collections::BTreeMap;

const WRITE_ROWS_EVENT: u8 = 30;
const UPDATE_ROWS_EVENT: u8 = 31;
const DELETE_ROWS_EVENT: u8 = 32;
const QUERY_EVENT: u8 = 2;

/// Decodes a raw binlog event into a `ChangeEvent`.
pub fn decode_binlog_event(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    if raw.payload.is_empty() {
        return Err(JikanError::ReplicationProtocol("empty binlog event".into()));
    }

    // Binlog event header is 19 bytes; the type code is byte 4 (0-indexed).
    // For simplicity in this stub, we read the first byte as a type tag.
    let event_type = raw.payload[0];
    match event_type {
        WRITE_ROWS_EVENT => decode_write_rows(raw),
        UPDATE_ROWS_EVENT => decode_update_rows(raw),
        DELETE_ROWS_EVENT => decode_delete_rows(raw),
        QUERY_EVENT => decode_query_event(raw),
        _ => {
            // Rotate, Format_description, and other control events carry
            // no row data and are not forwarded to the sink.
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

/// Decodes a WRITE_ROWS_EVENT into an insert change event.
fn decode_write_rows(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    Ok(ChangeEvent {
        position: raw.position,
        table: TableId::new("db", "unknown"),
        kind: EventKind::Insert,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(0))),
        after: Some(BTreeMap::new()),
        before: None,
        committed_at: None,
    })
}

/// Decodes an UPDATE_ROWS_EVENT into an update change event.
fn decode_update_rows(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    Ok(ChangeEvent {
        position: raw.position,
        table: TableId::new("db", "unknown"),
        kind: EventKind::Update,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(0))),
        after: Some(BTreeMap::new()),
        before: Some(BTreeMap::new()),
        committed_at: None,
    })
}

/// Decodes a DELETE_ROWS_EVENT into a delete change event.
fn decode_delete_rows(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    Ok(ChangeEvent {
        position: raw.position,
        table: TableId::new("db", "unknown"),
        kind: EventKind::Delete,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(0))),
        after: None,
        before: Some(BTreeMap::new()),
        committed_at: None,
    })
}

/// Decodes a QUERY_EVENT carrying DDL into a DDL change event.
fn decode_query_event(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    // QUERY_EVENT carries DDL. We emit it as an EventKind::Ddl so the pipeline
    // can pause and re-read the schema before continuing.
    Ok(ChangeEvent {
        position: raw.position,
        table: TableId::new("", ""),
        kind: EventKind::Ddl,
        primary_key: None,
        after: None,
        before: None,
        committed_at: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jikan_core::position::{GtidSet, Position};

    fn raw(event_type: u8) -> RawEvent {
        RawEvent {
            position: Position::Gtid(GtidSet("uuid:1".into())),
            payload: bytes::Bytes::from(vec![event_type]),
        }
    }

    #[test]
    fn write_rows_decoded_as_insert() {
        let ev = decode_binlog_event(raw(WRITE_ROWS_EVENT)).unwrap();
        assert_eq!(ev.kind, EventKind::Insert);
    }

    #[test]
    fn update_rows_decoded_as_update() {
        let ev = decode_binlog_event(raw(UPDATE_ROWS_EVENT)).unwrap();
        assert_eq!(ev.kind, EventKind::Update);
    }

    #[test]
    fn delete_rows_decoded_as_delete() {
        let ev = decode_binlog_event(raw(DELETE_ROWS_EVENT)).unwrap();
        assert_eq!(ev.kind, EventKind::Delete);
    }

    #[test]
    fn query_event_decoded_as_ddl() {
        let ev = decode_binlog_event(raw(QUERY_EVENT)).unwrap();
        assert_eq!(ev.kind, EventKind::Ddl);
    }

    #[test]
    fn empty_payload_returns_error() {
        let raw = RawEvent {
            position: Position::Gtid(GtidSet("uuid:1".into())),
            payload: bytes::Bytes::new(),
        };
        assert!(decode_binlog_event(raw).is_err());
    }

    #[test]
    fn unknown_event_type_does_not_error() {
        let ev = decode_binlog_event(raw(99)).unwrap();
        assert_eq!(ev.kind, EventKind::Insert);
        assert!(ev.primary_key.is_none());
    }
}
