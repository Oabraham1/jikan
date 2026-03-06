// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! pgoutput logical replication protocol decoder.
//!
//! Decodes the binary wire format documented in the PostgreSQL source under
//! `src/backend/replication/pgoutput/pgoutput.c`. Each message begins with
//! a single byte tag. RELATION messages carry the schema needed to decode
//! subsequent row messages — the decoder maintains a relation cache keyed
//! by OID so that INSERT/UPDATE/DELETE messages can be fully resolved.

use bytes::{Buf, Bytes};
use jikan_core::{
    error::JikanError,
    event::{ChangeEvent, EventKind, RawEvent},
    position::Position,
    table::{ColumnValue, PrimaryKey, TableId},
};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

/// A cached description of one column within a relation.
#[derive(Debug, Clone)]
pub struct ColumnDesc {
    /// The column name as reported by the RELATION message.
    pub name: String,
    /// The PostgreSQL type OID. Used to select the correct value decoder.
    pub type_oid: u32,
    /// Whether this column is part of the replica identity (primary key or
    /// REPLICA IDENTITY FULL index).
    pub is_key: bool,
}

/// A cached relation descriptor, populated from RELATION messages.
#[derive(Debug, Clone)]
pub struct RelationDesc {
    /// Fully-qualified table identifier.
    pub table: TableId,
    /// Columns in attribute order (attnum ascending).
    pub columns: Vec<ColumnDesc>,
}

impl RelationDesc {
    /// Returns the names of all key columns, in attribute order.
    pub fn key_columns(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }
}

/// A shared, thread-safe cache of relation descriptors indexed by OID.
///
/// The replication stream sends a RELATION message before the first
/// row message for each relation and again whenever the schema changes.
/// The decoder updates this cache on each RELATION message and uses it
/// to resolve INSERT/UPDATE/DELETE messages.
#[derive(Clone, Default)]
pub struct RelationCache(Arc<RwLock<HashMap<u32, RelationDesc>>>);

impl RelationCache {
    /// Creates an empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces the descriptor for `oid`.
    pub fn upsert(&self, oid: u32, desc: RelationDesc) {
        let mut guard = self.0.write().unwrap_or_else(|e| e.into_inner());
        guard.insert(oid, desc);
    }

    /// Returns a clone of the descriptor for `oid`, if present.
    pub fn get(&self, oid: u32) -> Option<RelationDesc> {
        let guard = self.0.read().unwrap_or_else(|e| e.into_inner());
        guard.get(&oid).cloned()
    }
}

/// Decodes one pgoutput message from `raw` using `cache` to resolve relation OIDs.
///
/// Returns `None` for control messages (BEGIN, COMMIT, TRUNCATE) that carry
/// no row data for the sink.
pub fn decode_pgoutput(
    raw: RawEvent,
    cache: &RelationCache,
) -> Result<Option<ChangeEvent>, JikanError> {
    let mut buf = raw.payload.clone();
    if !buf.has_remaining() {
        return Err(JikanError::ReplicationProtocol(
            "empty pgoutput message".into(),
        ));
    }

    let tag = buf.get_u8();
    match tag {
        b'R' => {
            decode_relation_message(buf, cache)?;
            Ok(None)
        }
        b'I' => Ok(Some(decode_insert(buf, raw.position, cache)?)),
        b'U' => Ok(Some(decode_update(buf, raw.position, cache)?)),
        b'D' => Ok(Some(decode_delete(buf, raw.position, cache)?)),
        b'B' | b'C' | b'T' => Ok(None),
        other => {
            tracing::debug!(tag = %char::from(other), "ignoring unknown pgoutput message type");
            Ok(None)
        }
    }
}

/// Parses a RELATION message and stores the result in `cache`.
fn decode_relation_message(mut buf: Bytes, cache: &RelationCache) -> Result<(), JikanError> {
    let oid = read_u32(&mut buf)?;
    let namespace = read_cstring(&mut buf)?;
    let name = read_cstring(&mut buf)?;
    let _replica_identity = read_u8(&mut buf)?;
    let col_count = read_u16(&mut buf)? as usize;

    let mut columns = Vec::with_capacity(col_count);
    for _ in 0..col_count {
        let flags = read_u8(&mut buf)?;
        let col_name = read_cstring(&mut buf)?;
        let type_oid = read_u32(&mut buf)?;
        let _atttypmod = read_u32(&mut buf)?;
        columns.push(ColumnDesc {
            name: col_name,
            type_oid,
            is_key: flags & 0x01 != 0,
        });
    }

    cache.upsert(
        oid,
        RelationDesc {
            table: TableId::new(namespace, name),
            columns,
        },
    );
    Ok(())
}

fn decode_insert(
    mut buf: Bytes,
    position: Position,
    cache: &RelationCache,
) -> Result<ChangeEvent, JikanError> {
    let oid = read_u32(&mut buf)?;
    let _tuple_type = read_u8(&mut buf)?; // always 'N' for INSERT

    let rel = require_relation(oid, cache)?;
    let after = decode_tuple(&mut buf, &rel.columns)?;
    let pk = extract_primary_key(&after, &rel)?;

    Ok(ChangeEvent {
        position,
        table: rel.table,
        kind: EventKind::Insert,
        primary_key: Some(pk),
        after: Some(after),
        before: None,
        committed_at: None,
    })
}

fn decode_update(
    mut buf: Bytes,
    position: Position,
    cache: &RelationCache,
) -> Result<ChangeEvent, JikanError> {
    let oid = read_u32(&mut buf)?;
    let rel = require_relation(oid, cache)?;

    // UPDATE may include an old tuple prefixed with 'O' (full old row) or
    // 'K' (old key columns only), depending on REPLICA IDENTITY setting.
    let next_byte = peek_u8(&buf)?;
    let before = if next_byte == b'O' || next_byte == b'K' {
        buf.advance(1);
        Some(decode_tuple(&mut buf, &rel.columns)?)
    } else {
        None
    };

    let _new_type = read_u8(&mut buf)?; // 'N'
    let after = decode_tuple(&mut buf, &rel.columns)?;
    let pk = extract_primary_key(&after, &rel)?;

    Ok(ChangeEvent {
        position,
        table: rel.table,
        kind: EventKind::Update,
        primary_key: Some(pk),
        after: Some(after),
        before,
        committed_at: None,
    })
}

fn decode_delete(
    mut buf: Bytes,
    position: Position,
    cache: &RelationCache,
) -> Result<ChangeEvent, JikanError> {
    let oid = read_u32(&mut buf)?;
    let rel = require_relation(oid, cache)?;

    let _tuple_type = read_u8(&mut buf)?; // 'O' or 'K'
    let before = decode_tuple(&mut buf, &rel.columns)?;
    let pk = extract_primary_key(&before, &rel)?;

    Ok(ChangeEvent {
        position,
        table: rel.table,
        kind: EventKind::Delete,
        primary_key: Some(pk),
        after: None,
        before: Some(before),
        committed_at: None,
    })
}

/// Decodes a tuple (row) from `buf` using the column descriptors from `rel`.
fn decode_tuple(
    buf: &mut Bytes,
    columns: &[ColumnDesc],
) -> Result<BTreeMap<String, ColumnValue>, JikanError> {
    let col_count = read_u16(buf)? as usize;
    if col_count != columns.len() {
        return Err(JikanError::ReplicationProtocol(format!(
            "tuple has {col_count} columns but relation cache says {}",
            columns.len()
        )));
    }

    let mut row = BTreeMap::new();
    for col in columns {
        let kind = read_u8(buf)?;
        let value = match kind {
            b'n' => ColumnValue::Null,
            b'u' => ColumnValue::Null, // unchanged toast; treat as null for now
            b't' => {
                let len = read_u32(buf)? as usize;
                if buf.remaining() < len {
                    return Err(JikanError::ReplicationProtocol(format!(
                        "column {} text value truncated: need {len} bytes, have {}",
                        col.name,
                        buf.remaining()
                    )));
                }
                let text_bytes = buf.copy_to_bytes(len);
                let text = String::from_utf8(text_bytes.to_vec()).map_err(|e| {
                    JikanError::ReplicationProtocol(format!(
                        "column {} is not valid UTF-8: {e}",
                        col.name
                    ))
                })?;
                decode_text_value(&text, col.type_oid)
            }
            other => {
                return Err(JikanError::ReplicationProtocol(format!(
                    "unknown tuple column kind byte: 0x{other:02x}"
                )))
            }
        };
        row.insert(col.name.clone(), value);
    }
    Ok(row)
}

/// Converts a text-form column value (as sent by pgoutput) into a `ColumnValue`.
///
/// pgoutput sends all column values as their text representation regardless of
/// the underlying binary type. We parse into a native Rust type where possible
/// and fall back to `ColumnValue::Text` for types we do not recognise.
fn decode_text_value(text: &str, type_oid: u32) -> ColumnValue {
    // PostgreSQL built-in type OIDs for the types we decode natively.
    // Full list: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
    match type_oid {
        16 => {
            // bool
            match text {
                "t" | "true" | "1" | "yes" | "on" => ColumnValue::Bool(true),
                _ => ColumnValue::Bool(false),
            }
        }
        20 | 21 | 23 | 26 => {
            // int8, int2, int4, oid
            text.parse::<i64>()
                .map(ColumnValue::Int)
                .unwrap_or_else(|_| ColumnValue::Text(text.to_owned()))
        }
        700 | 701 => {
            // float4, float8
            text.parse::<f64>()
                .map(ColumnValue::Float)
                .unwrap_or_else(|_| ColumnValue::Text(text.to_owned()))
        }
        17 => {
            // bytea — text form is hex-encoded with \x prefix
            let hex_str = text.strip_prefix("\\x").unwrap_or(text);
            hex::decode(hex_str)
                .map(ColumnValue::Bytes)
                .unwrap_or_else(|_| ColumnValue::Text(text.to_owned()))
        }
        _ => ColumnValue::Text(text.to_owned()),
    }
}

/// Extracts the primary key columns from a decoded row map.
fn extract_primary_key(
    row: &BTreeMap<String, ColumnValue>,
    rel: &RelationDesc,
) -> Result<PrimaryKey, JikanError> {
    let key_cols = rel.key_columns();
    if key_cols.is_empty() {
        return Err(JikanError::ReplicationProtocol(format!(
            "relation {}.{} has no key columns; configure REPLICA IDENTITY",
            rel.table.schema, rel.table.name
        )));
    }
    let mut pk_map = BTreeMap::new();
    for col in key_cols {
        let value = row.get(col).cloned().unwrap_or(ColumnValue::Null);
        pk_map.insert(col.to_owned(), value);
    }
    Ok(PrimaryKey(pk_map))
}

fn require_relation(oid: u32, cache: &RelationCache) -> Result<RelationDesc, JikanError> {
    cache.get(oid).ok_or_else(|| {
        JikanError::ReplicationProtocol(format!(
            "relation OID {oid} not in cache; RELATION message may have been missed"
        ))
    })
}

// --- Low-level buffer readers ---

fn read_u8(buf: &mut Bytes) -> Result<u8, JikanError> {
    if buf.remaining() < 1 {
        return Err(JikanError::ReplicationProtocol(
            "buffer underflow reading u8".into(),
        ));
    }
    Ok(buf.get_u8())
}

fn read_u16(buf: &mut Bytes) -> Result<u16, JikanError> {
    if buf.remaining() < 2 {
        return Err(JikanError::ReplicationProtocol(
            "buffer underflow reading u16".into(),
        ));
    }
    Ok(buf.get_u16())
}

fn read_u32(buf: &mut Bytes) -> Result<u32, JikanError> {
    if buf.remaining() < 4 {
        return Err(JikanError::ReplicationProtocol(
            "buffer underflow reading u32".into(),
        ));
    }
    Ok(buf.get_u32())
}

fn peek_u8(buf: &Bytes) -> Result<u8, JikanError> {
    buf.first()
        .copied()
        .ok_or_else(|| JikanError::ReplicationProtocol("buffer underflow peeking u8".into()))
}

/// Reads a null-terminated C string from `buf`.
fn read_cstring(buf: &mut Bytes) -> Result<String, JikanError> {
    let end = buf.iter().position(|&b| b == 0).ok_or_else(|| {
        JikanError::ReplicationProtocol("unterminated cstring in pgoutput".into())
    })?;
    let s = String::from_utf8(buf[..end].to_vec())
        .map_err(|e| JikanError::ReplicationProtocol(format!("cstring not UTF-8: {e}")))?;
    buf.advance(end + 1); // consume string + null terminator
    Ok(s)
}

/// Benchmark helpers for the pgoutput decoder.
///
/// Gated behind the `bench` feature to avoid polluting the public API in
/// normal builds. Re-exported from `crate::codec_bench` for use in
/// criterion benchmarks.
#[cfg(feature = "bench")]
pub mod bench {
    use super::*;

    /// Constructs a synthetic pgoutput RELATION + INSERT payload pair for benchmarking.
    ///
    /// Returns the RELATION payload that must be decoded first to populate the
    /// relation cache, and the INSERT payload to benchmark.
    pub fn make_insert_payload() -> (bytes::Bytes, bytes::Bytes) {
        let oid: u32 = 16384;

        // Build RELATION message
        let mut rel = vec![b'R'];
        rel.extend_from_slice(&oid.to_be_bytes());
        rel.extend_from_slice(b"public\0");
        rel.extend_from_slice(b"bench_table\0");
        rel.push(100); // replica identity DEFAULT
        rel.extend_from_slice(&1u16.to_be_bytes()); // 1 column
        rel.push(0x01); // is_key flag
        rel.extend_from_slice(b"id\0");
        rel.extend_from_slice(&23u32.to_be_bytes()); // int4 OID
        rel.extend_from_slice(&0u32.to_be_bytes()); // atttypmod

        // Build INSERT message
        let mut ins = vec![b'I'];
        ins.extend_from_slice(&oid.to_be_bytes());
        ins.push(b'N');
        ins.extend_from_slice(&1u16.to_be_bytes()); // 1 column
        ins.push(b't');
        let text = b"42";
        ins.extend_from_slice(&(text.len() as u32).to_be_bytes());
        ins.extend_from_slice(text);

        (bytes::Bytes::from(rel), bytes::Bytes::from(ins))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jikan_core::position::{Lsn, Position};

    fn pos() -> Position {
        Position::Lsn(Lsn(1000))
    }

    /// Builds a minimal RELATION message for a two-column table with one key column.
    fn relation_message(oid: u32) -> Bytes {
        let mut v = vec![b'R'];
        v.extend_from_slice(&oid.to_be_bytes());
        v.extend_from_slice(b"public\0");
        v.extend_from_slice(b"users\0");
        v.push(100u8); // replica identity DEFAULT
        v.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
                                                  // column 1: id (int4, key)
        v.push(0x01); // is_key flag
        v.extend_from_slice(b"id\0");
        v.extend_from_slice(&23u32.to_be_bytes()); // int4 OID
        v.extend_from_slice(&0u32.to_be_bytes()); // atttypmod
                                                  // column 2: name (text, not key)
        v.push(0x00);
        v.extend_from_slice(b"name\0");
        v.extend_from_slice(&25u32.to_be_bytes()); // text OID
        v.extend_from_slice(&0u32.to_be_bytes());
        Bytes::from(v)
    }

    /// Builds a minimal INSERT message for the relation above.
    fn insert_message(oid: u32, id: i64, name: &str) -> Bytes {
        let mut v = vec![b'I'];
        v.extend_from_slice(&oid.to_be_bytes());
        v.push(b'N');
        v.extend_from_slice(&2u16.to_be_bytes()); // 2 columns
                                                  // id column
        v.push(b't');
        let id_str = id.to_string();
        v.extend_from_slice(&(id_str.len() as u32).to_be_bytes());
        v.extend_from_slice(id_str.as_bytes());
        // name column
        v.push(b't');
        v.extend_from_slice(&(name.len() as u32).to_be_bytes());
        v.extend_from_slice(name.as_bytes());
        Bytes::from(v)
    }

    /// Builds an UPDATE message with no old tuple.
    fn update_message(oid: u32, id: i64, name: &str) -> Bytes {
        let mut v = vec![b'U'];
        v.extend_from_slice(&oid.to_be_bytes());
        // No old tuple, go straight to 'N' + new tuple
        v.push(b'N');
        v.extend_from_slice(&2u16.to_be_bytes());
        v.push(b't');
        let id_str = id.to_string();
        v.extend_from_slice(&(id_str.len() as u32).to_be_bytes());
        v.extend_from_slice(id_str.as_bytes());
        v.push(b't');
        v.extend_from_slice(&(name.len() as u32).to_be_bytes());
        v.extend_from_slice(name.as_bytes());
        Bytes::from(v)
    }

    /// Builds a DELETE message.
    fn delete_message(oid: u32, id: i64, name: &str) -> Bytes {
        let mut v = vec![b'D'];
        v.extend_from_slice(&oid.to_be_bytes());
        v.push(b'O'); // old tuple type
        v.extend_from_slice(&2u16.to_be_bytes());
        v.push(b't');
        let id_str = id.to_string();
        v.extend_from_slice(&(id_str.len() as u32).to_be_bytes());
        v.extend_from_slice(id_str.as_bytes());
        v.push(b't');
        v.extend_from_slice(&(name.len() as u32).to_be_bytes());
        v.extend_from_slice(name.as_bytes());
        Bytes::from(v)
    }

    fn raw(payload: Bytes) -> RawEvent {
        RawEvent {
            position: pos(),
            payload,
        }
    }

    #[test]
    fn relation_message_populates_cache() {
        let cache = RelationCache::new();
        let payload = relation_message(42);
        decode_pgoutput(raw(payload), &cache).unwrap();
        let rel = cache.get(42).expect("relation should be cached");
        assert_eq!(rel.table, TableId::new("public", "users"));
        assert_eq!(rel.columns.len(), 2);
        assert!(rel.columns[0].is_key);
        assert!(!rel.columns[1].is_key);
    }

    #[test]
    fn insert_decoded_with_correct_pk_and_columns() {
        let cache = RelationCache::new();
        decode_pgoutput(raw(relation_message(42)), &cache).unwrap();
        let payload = insert_message(42, 7, "Alice");
        let event = decode_pgoutput(raw(payload), &cache).unwrap().unwrap();
        assert_eq!(event.kind, EventKind::Insert);
        let pk = event.primary_key.unwrap();
        assert_eq!(pk.0.get("id"), Some(&ColumnValue::Int(7)));
        let after = event.after.unwrap();
        assert_eq!(after.get("name"), Some(&ColumnValue::Text("Alice".into())));
    }

    #[test]
    fn update_decoded_correctly() {
        let cache = RelationCache::new();
        decode_pgoutput(raw(relation_message(42)), &cache).unwrap();
        let payload = update_message(42, 3, "Bob");
        let event = decode_pgoutput(raw(payload), &cache).unwrap().unwrap();
        assert_eq!(event.kind, EventKind::Update);
        assert!(event.before.is_none()); // no old tuple in this message
        let after = event.after.unwrap();
        assert_eq!(after.get("id"), Some(&ColumnValue::Int(3)));
        assert_eq!(after.get("name"), Some(&ColumnValue::Text("Bob".into())));
    }

    #[test]
    fn delete_decoded_correctly() {
        let cache = RelationCache::new();
        decode_pgoutput(raw(relation_message(42)), &cache).unwrap();
        let payload = delete_message(42, 5, "Eve");
        let event = decode_pgoutput(raw(payload), &cache).unwrap().unwrap();
        assert_eq!(event.kind, EventKind::Delete);
        assert!(event.after.is_none());
        let before = event.before.unwrap();
        assert_eq!(before.get("id"), Some(&ColumnValue::Int(5)));
        assert_eq!(before.get("name"), Some(&ColumnValue::Text("Eve".into())));
    }

    #[test]
    fn insert_without_relation_in_cache_is_error() {
        let cache = RelationCache::new();
        let payload = insert_message(99, 1, "Bob");
        let result = decode_pgoutput(raw(payload), &cache);
        assert!(result.is_err());
    }

    #[test]
    fn begin_and_commit_return_none() {
        let cache = RelationCache::new();
        for tag in [b'B', b'C'] {
            let mut v = vec![tag];
            v.extend_from_slice(&[0u8; 20]); // padding
            let result = decode_pgoutput(raw(Bytes::from(v)), &cache).unwrap();
            assert!(result.is_none());
        }
    }

    #[test]
    fn empty_payload_is_error() {
        let cache = RelationCache::new();
        let raw_event = RawEvent {
            position: Position::Lsn(Lsn(1)),
            payload: bytes::Bytes::new(),
        };
        assert!(decode_pgoutput(raw_event, &cache).is_err());
    }

    #[test]
    fn null_column_decoded() {
        let cache = RelationCache::new();
        decode_pgoutput(raw(relation_message(42)), &cache).unwrap();

        // INSERT with id=1, name=null
        let mut v = vec![b'I'];
        v.extend_from_slice(&42u32.to_be_bytes());
        v.push(b'N');
        v.extend_from_slice(&2u16.to_be_bytes());
        // id column: text
        v.push(b't');
        let id_str = b"1";
        v.extend_from_slice(&(id_str.len() as u32).to_be_bytes());
        v.extend_from_slice(id_str);
        // name column: null
        v.push(b'n');

        let event = decode_pgoutput(raw(Bytes::from(v)), &cache)
            .unwrap()
            .unwrap();
        let after = event.after.unwrap();
        assert_eq!(after.get("name"), Some(&ColumnValue::Null));
    }

    #[test]
    fn bool_type_decoded() {
        let text_true = decode_text_value("t", 16);
        assert_eq!(text_true, ColumnValue::Bool(true));
        let text_false = decode_text_value("f", 16);
        assert_eq!(text_false, ColumnValue::Bool(false));
    }

    #[test]
    fn float_type_decoded() {
        let val = decode_text_value("1.25", 701);
        assert_eq!(val, ColumnValue::Float(1.25));
    }

    #[test]
    fn bytea_type_decoded() {
        let val = decode_text_value("\\x48454c4c4f", 17);
        assert_eq!(val, ColumnValue::Bytes(b"HELLO".to_vec()));
    }
}
