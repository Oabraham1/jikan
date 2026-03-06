// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Snapshot chunk reading under REPEATABLE READ isolation for MySQL.
//!
//! Each chunk is read inside a `START TRANSACTION WITH CONSISTENT SNAPSHOT`
//! block, which pins the read view to the moment the transaction begins
//! (Berenson et al. 1995). This prevents read skew between chunks when
//! concurrent writes are modifying the same table.

use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, EventKind},
    position::Position,
    source::SnapshotChunkStream,
    table::{ColumnValue, PrimaryKey, TableId, TableSchema},
};
use tracing::instrument;

/// Queries table schemas from `information_schema`.
pub async fn query_table_schemas(
    url: &str,
    databases: &[String],
) -> Result<Vec<TableSchema>, JikanError> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    let db_filter = if databases.is_empty() {
        "TABLE_SCHEMA NOT IN ('information_schema','mysql','performance_schema','sys')".to_owned()
    } else {
        let quoted: Vec<String> = databases.iter().map(|d| format!("'{d}'")).collect();
        format!("TABLE_SCHEMA IN ({})", quoted.join(","))
    };

    use mysql_async::prelude::Queryable;
    let rows: Vec<mysql_async::Row> = conn
        .query(format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_KEY \
             FROM information_schema.COLUMNS \
             WHERE {db_filter} \
             ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION"
        ))
        .await
        .map_err(|e| JikanError::Snapshot(format!("schema query: {e}")))?;

    let mut map: std::collections::BTreeMap<(String, String), (Vec<String>, Vec<String>)> =
        std::collections::BTreeMap::new();

    for row in &rows {
        let schema: String = row.get("TABLE_SCHEMA").unwrap_or_default();
        let table: String = row.get("TABLE_NAME").unwrap_or_default();
        let col: String = row.get("COLUMN_NAME").unwrap_or_default();
        let key: String = row.get("COLUMN_KEY").unwrap_or_default();
        let entry = map.entry((schema, table)).or_default();
        entry.0.push(col.clone());
        if key == "PRI" {
            entry.1.push(col);
        }
    }

    let schemas = map
        .into_iter()
        .map(|((schema, name), (columns, pk_cols))| TableSchema {
            table: TableId::new(schema, name),
            columns,
            primary_key_columns: pk_cols,
        })
        .collect();

    pool.disconnect()
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;
    Ok(schemas)
}

/// Reads one chunk of rows from the table described by `cursor`.
///
/// Adya et al. (2000): the snapshot transaction is read-only, equivalent to
/// PL-3 under MySQL's snapshot isolation. Delivering a snapshot row after a
/// conflicting binlog event at a higher GTID would be an anti-dependency;
/// the merger prevents this.
#[instrument(skip(url, cursor))]
pub async fn read_chunk(
    url: &str,
    cursor: &ChunkCursor,
    chunk_size: u32,
) -> Result<SnapshotChunkStream, JikanError> {
    let pool = mysql_async::Pool::new(url);
    let mut conn = pool
        .get_conn()
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    use mysql_async::prelude::Queryable;
    conn.query_drop("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        .await
        .map_err(|e| JikanError::Snapshot(format!("begin snapshot transaction: {e}")))?;

    let table = &cursor.table;
    let snapshot_position = cursor.snapshot_position.clone();

    let rows: Vec<mysql_async::Row> = if let Some(last_pk) = &cursor.last_pk {
        let last_id = match last_pk.0.values().next() {
            Some(ColumnValue::Int(id)) => *id,
            _ => {
                return Err(JikanError::Snapshot(
                    "only single integer PKs supported".into(),
                ))
            }
        };
        conn.exec(
            format!(
                "SELECT * FROM `{}`.`{}` WHERE id > ? ORDER BY id LIMIT ?",
                table.schema, table.name
            ),
            (last_id, chunk_size),
        )
        .await
        .map_err(|e| JikanError::Snapshot(format!("chunk query: {e}")))?
    } else {
        conn.exec(
            format!(
                "SELECT * FROM `{}`.`{}` ORDER BY id LIMIT ?",
                table.schema, table.name
            ),
            (chunk_size,),
        )
        .await
        .map_err(|e| JikanError::Snapshot(format!("chunk query: {e}")))?
    };

    let events: Vec<Result<ChangeEvent, JikanError>> = rows
        .iter()
        .map(|row| {
            let id: i64 = row.get(0).unwrap_or(0);
            Ok(row_to_insert_event(table, &snapshot_position, id))
        })
        .collect();

    conn.query_drop("COMMIT")
        .await
        .map_err(|e| JikanError::Snapshot(format!("commit snapshot transaction: {e}")))?;

    pool.disconnect()
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    let stream = futures::stream::iter(events);
    Ok(Box::pin(stream))
}

/// Converts a snapshot row into an insert event for delivery to the merger.
fn row_to_insert_event(table: &TableId, position: &Position, id: i64) -> ChangeEvent {
    ChangeEvent {
        position: position.clone(),
        table: table.clone(),
        kind: EventKind::Insert,
        primary_key: Some(PrimaryKey::single("id", ColumnValue::Int(id))),
        after: None,
        before: None,
        committed_at: None,
    }
}
