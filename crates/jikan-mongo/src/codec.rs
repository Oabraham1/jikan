// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! MongoDB change event decoder.
//!
//! Change stream documents have a well-defined structure documented at
//! <https://www.mongodb.com/docs/manual/reference/change-events/>. The
//! `operationType` field determines whether an event is an insert, update,
//! replace, or delete.

use jikan_core::{
    error::JikanError,
    event::{ChangeEvent, EventKind, RawEvent},
    position::{OplogTimestamp, Position},
    table::{ColumnValue, PrimaryKey, TableId},
};
use mongodb::bson::Document;

/// Decodes a raw change stream event into a `ChangeEvent`.
pub fn decode_change_event(raw: RawEvent) -> Result<ChangeEvent, JikanError> {
    let doc: Document = bson::from_slice(&raw.payload)
        .map_err(|e| JikanError::ReplicationProtocol(format!("bson decode: {e}")))?;

    let op_type = doc
        .get_str("operationType")
        .map_err(|e| JikanError::ReplicationProtocol(format!("missing operationType: {e}")))?;

    let ns = doc
        .get_document("ns")
        .map_err(|e| JikanError::ReplicationProtocol(format!("missing ns: {e}")))?;
    let db = ns.get_str("db").unwrap_or("");
    let coll = ns.get_str("coll").unwrap_or("");

    let doc_key = doc.get_document("documentKey").ok();
    let id_str = doc_key
        .and_then(|dk| dk.get("_id"))
        .map(|v| v.to_string())
        .unwrap_or_default();

    let position = cluster_time_from_doc(&doc).unwrap_or(raw.position);

    let kind = match op_type {
        "insert" | "replace" => EventKind::Insert,
        "update" => EventKind::Update,
        "delete" => EventKind::Delete,
        "drop" | "rename" | "dropDatabase" | "invalidate" => EventKind::Ddl,
        _ => EventKind::Insert,
    };

    Ok(ChangeEvent {
        position,
        table: TableId::new(db, coll),
        kind,
        primary_key: Some(PrimaryKey::single("_id", ColumnValue::Text(id_str))),
        after: None,
        before: None,
        committed_at: None,
    })
}

/// Extracts the `clusterTime` field from a change stream document and
/// converts it to a `Position`.
fn cluster_time_from_doc(doc: &Document) -> Option<Position> {
    let ts = doc.get_timestamp("clusterTime").ok()?;
    Some(Position::OplogTs(OplogTimestamp {
        seconds: ts.time,
        ordinal: ts.increment,
        term: 0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jikan_core::position::Position;

    fn raw_from_doc(doc: &Document) -> RawEvent {
        let bytes = bson::to_vec(doc).expect("test bson serialization");
        RawEvent {
            position: Position::OplogTs(OplogTimestamp {
                seconds: 1000,
                ordinal: 1,
                term: 1,
            }),
            payload: bytes::Bytes::from(bytes),
        }
    }

    #[test]
    fn insert_op_decoded() {
        let doc = bson::doc! {
            "operationType": "insert",
            "ns": { "db": "mydb", "coll": "users" },
            "documentKey": { "_id": "abc" },
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode insert");
        assert_eq!(ev.kind, EventKind::Insert);
        assert_eq!(ev.table, TableId::new("mydb", "users"));
    }

    #[test]
    fn delete_op_decoded() {
        let doc = bson::doc! {
            "operationType": "delete",
            "ns": { "db": "mydb", "coll": "orders" },
            "documentKey": { "_id": "xyz" },
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode delete");
        assert_eq!(ev.kind, EventKind::Delete);
    }

    #[test]
    fn update_op_decoded() {
        let doc = bson::doc! {
            "operationType": "update",
            "ns": { "db": "analytics", "coll": "events" },
            "documentKey": { "_id": "u1" },
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode update");
        assert_eq!(ev.kind, EventKind::Update);
    }

    #[test]
    fn replace_op_decoded_as_insert() {
        let doc = bson::doc! {
            "operationType": "replace",
            "ns": { "db": "mydb", "coll": "users" },
            "documentKey": { "_id": "r1" },
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode replace");
        assert_eq!(ev.kind, EventKind::Insert);
    }

    #[test]
    fn ddl_drop_op_decoded() {
        let doc = bson::doc! {
            "operationType": "drop",
            "ns": { "db": "mydb", "coll": "old_table" },
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode drop");
        assert_eq!(ev.kind, EventKind::Ddl);
    }

    #[test]
    fn cluster_time_extracted_when_present() {
        let doc = bson::doc! {
            "operationType": "insert",
            "ns": { "db": "mydb", "coll": "users" },
            "documentKey": { "_id": "ct1" },
            "clusterTime": bson::Bson::Timestamp(bson::Timestamp { time: 2000, increment: 5 }),
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode with clusterTime");
        match ev.position {
            Position::OplogTs(ts) => {
                assert_eq!(ts.seconds, 2000);
                assert_eq!(ts.ordinal, 5);
            }
            other => panic!("expected OplogTs, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_raw_position_without_cluster_time() {
        let doc = bson::doc! {
            "operationType": "insert",
            "ns": { "db": "mydb", "coll": "users" },
            "documentKey": { "_id": "fb1" },
        };
        let ev = decode_change_event(raw_from_doc(&doc)).expect("decode without clusterTime");
        match ev.position {
            Position::OplogTs(ts) => {
                assert_eq!(ts.seconds, 1000);
                assert_eq!(ts.ordinal, 1);
            }
            other => panic!("expected OplogTs, got {other:?}"),
        }
    }
}
