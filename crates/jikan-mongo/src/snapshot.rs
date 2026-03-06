// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Change stream opening and collection snapshot reading.

use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, EventKind},
    position::{OplogTimestamp, Position},
    source::{EventStream, SnapshotChunkStream},
    table::{ColumnValue, PrimaryKey, TableId, TableSchema},
};
use mongodb::{
    bson::{doc, Bson, Document, Timestamp},
    options::{AggregateOptions, ChangeStreamOptions, ReadConcern},
    Client,
};
use tracing::instrument;

/// Opens a change stream starting at `start_ts`, filtered to the given databases.
///
/// Chandy & Lamport (1985): the stream must be opened at `start_ts` before
/// any snapshot documents are read. Documents from the snapshot are consistent
/// with the state at `start_ts`; the stream delivers all changes after that point.
pub async fn open_change_stream(
    client: &Client,
    databases: &[String],
    start_ts: OplogTimestamp,
) -> Result<EventStream, JikanError> {
    let ts = Timestamp {
        time: start_ts.seconds,
        increment: start_ts.ordinal,
    };

    let options = ChangeStreamOptions::builder()
        .start_at_operation_time(Some(ts))
        .build();

    // A deployment-level change stream is opened when databases is empty;
    // otherwise a per-database stream is opened for each listed database.
    // For simplicity this stub returns an error; Phase 05 integration wires
    // in the full stream multiplexing.
    let _ = (client, databases, options);
    Err(JikanError::ReplicationProtocol(
        "change stream construction is completed in Phase 05 integration".into(),
    ))
}

/// Returns the schemas of all collections in the listed databases.
pub async fn query_collection_schemas(
    client: &Client,
    databases: &[String],
) -> Result<Vec<TableSchema>, JikanError> {
    let db_names: Vec<String> = if databases.is_empty() {
        client
            .list_database_names(None, None)
            .await
            .map_err(|e| JikanError::Snapshot(e.to_string()))?
            .into_iter()
            .filter(|n| !matches!(n.as_str(), "admin" | "config" | "local"))
            .collect()
    } else {
        databases.to_vec()
    };

    let mut schemas = Vec::new();
    for db_name in &db_names {
        let db = client.database(db_name);
        let collections = db
            .list_collection_names(None)
            .await
            .map_err(|e| JikanError::Snapshot(format!("list collections in {db_name}: {e}")))?;

        for coll_name in collections {
            // MongoDB collections are schemaless; we represent the schema as
            // a single wildcard column. The connector infers the actual fields
            // from the first document read during snapshot.
            schemas.push(TableSchema {
                table: TableId::new(db_name.clone(), coll_name),
                columns: vec!["*".into()],
                primary_key_columns: vec!["_id".into()],
            });
        }
    }
    Ok(schemas)
}

/// Reads one chunk of documents from the collection described by `cursor`.
///
/// Berenson et al. (1995): the `snapshot` read concern pins the read to
/// `snapshot_position.atClusterTime`, giving repeatable-read semantics.
#[instrument(skip(client, cursor))]
pub async fn read_chunk(
    client: &Client,
    cursor: &ChunkCursor,
    chunk_size: u32,
) -> Result<SnapshotChunkStream, JikanError> {
    let table = &cursor.table;
    let snapshot_ts = match &cursor.snapshot_position {
        Position::OplogTs(ts) => *ts,
        other => {
            return Err(JikanError::Snapshot(format!(
                "mongodb snapshot requires OplogTs position, got {other:?}"
            )))
        }
    };

    let db = client.database(&table.schema);
    let coll = db.collection::<Document>(&table.name);

    let cluster_ts = Timestamp {
        time: snapshot_ts.seconds,
        increment: snapshot_ts.ordinal,
    };

    // Sort by _id and apply keyset pagination to avoid OFFSET.
    let mut pipeline = vec![
        doc! { "$sort": { "_id": 1 } },
        doc! { "$limit": chunk_size as i64 },
    ];

    if let Some(last_pk) = &cursor.last_pk {
        if let Some(ColumnValue::Text(last_id)) = last_pk.0.get("_id") {
            pipeline.insert(0, doc! { "$match": { "_id": { "$gt": last_id.clone() } } });
        }
    }

    let options = AggregateOptions::builder()
        .read_concern(ReadConcern::snapshot())
        .build();
    let _ = cluster_ts; // atClusterTime is set via session in Phase 05 integration

    let mut cursor_result = coll
        .aggregate(pipeline, options)
        .await
        .map_err(|e| JikanError::Snapshot(format!("snapshot aggregate: {e}")))?;

    let mut events = Vec::new();
    while cursor_result
        .advance()
        .await
        .map_err(|e| JikanError::Snapshot(format!("cursor advance: {e}")))?
    {
        let doc = cursor_result
            .deserialize_current()
            .map_err(|e| JikanError::Snapshot(format!("deserialize doc: {e}")))?;

        let id = doc.get("_id").cloned().unwrap_or(Bson::Null);
        let id_str = id.to_string();

        events.push(Ok(ChangeEvent {
            position: cursor.snapshot_position.clone(),
            table: table.clone(),
            kind: EventKind::Insert,
            primary_key: Some(PrimaryKey::single("_id", ColumnValue::Text(id_str))),
            after: None, // full document mapping added in Phase 06
            before: None,
            committed_at: None,
        }));
    }

    Ok(Box::pin(futures::stream::iter(events)))
}
