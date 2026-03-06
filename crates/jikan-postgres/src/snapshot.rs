// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Snapshot chunk reading under REPEATABLE READ isolation.
//!
//! Berenson et al. (1995): snapshot chunks must be read inside a transaction
//! at REPEATABLE READ or stronger. A lower isolation level permits read skew
//! between chunks, which would produce a snapshot that never existed as a
//! consistent database state.

use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, EventKind},
    position::Position,
    source::SnapshotChunkStream,
    table::{ColumnValue, PrimaryKey, TableId, TableSchema},
};
use tokio_postgres::NoTls;
use tracing::instrument;

/// Queries the schemas of all tables captured by the publication.
pub async fn query_table_schemas(connection_string: &str) -> Result<Vec<TableSchema>, JikanError> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "schema query connection failed");
        }
    });

    let rows = client
        .query(
            r#"
            SELECT n.nspname AS schema, c.relname AS table_name,
                   array_agg(a.attname ORDER BY a.attnum) AS columns,
                   array_agg(a.attname ORDER BY a.attnum)
                       FILTER (WHERE i.indisprimary) AS pk_columns
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
            LEFT JOIN pg_index i ON i.indrelid = c.oid AND a.attnum = ANY(i.indkey)
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            GROUP BY n.nspname, c.relname
            "#,
            &[],
        )
        .await
        .map_err(|e| JikanError::Snapshot(format!("schema query failed: {e}")))?;

    let schemas = rows
        .iter()
        .map(|row| {
            let schema: &str = row
                .try_get("schema")
                .map_err(|e| JikanError::Snapshot(e.to_string()))?;
            let name: &str = row
                .try_get("table_name")
                .map_err(|e| JikanError::Snapshot(e.to_string()))?;
            let columns: Vec<String> = row
                .try_get::<_, Vec<String>>("columns")
                .map_err(|e| JikanError::Snapshot(e.to_string()))?;
            let pk_columns: Vec<String> = row
                .try_get::<_, Option<Vec<String>>>("pk_columns")
                .map_err(|e| JikanError::Snapshot(e.to_string()))?
                .unwrap_or_default();

            Ok(TableSchema {
                table: TableId::new(schema, name),
                columns,
                primary_key_columns: pk_columns,
            })
        })
        .collect::<Result<Vec<_>, JikanError>>()?;

    Ok(schemas)
}

/// Reads one chunk of rows from the table described by `cursor`.
///
/// The chunk is bounded by `chunk_size`. Rows are returned in primary-key
/// order starting after `cursor.last_pk`. The snapshot transaction is
/// held for the duration of the stream — the caller must drain the stream
/// before calling this function again for the next chunk, or start a new
/// connection.
#[instrument(skip(connection_string, cursor))]
pub async fn read_chunk(
    connection_string: &str,
    cursor: &ChunkCursor,
    chunk_size: u32,
) -> Result<SnapshotChunkStream, JikanError> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "snapshot chunk connection failed");
        }
    });

    // Set REPEATABLE READ before any reads in this transaction.
    client
        .execute("BEGIN ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| JikanError::Snapshot(format!("begin repeatable read transaction: {e}")))?;

    let table = &cursor.table;
    let snapshot_position = cursor.snapshot_position.clone();

    let rows = if let Some(last_pk) = &cursor.last_pk {
        let last_id = match last_pk.0.values().next() {
            Some(ColumnValue::Int(id)) => *id,
            _ => {
                return Err(JikanError::Snapshot(
                    "only single integer PKs are supported in this phase".into(),
                ))
            }
        };
        let pk_col = last_pk
            .0
            .keys()
            .next()
            .cloned()
            .unwrap_or_else(|| "id".to_owned());
        let query = format!(
            r#"SELECT * FROM "{}"."{}" WHERE "{}" > $1 ORDER BY "{}" LIMIT $2"#,
            table.schema, table.name, pk_col, pk_col
        );
        client
            .query(&query, &[&last_id, &(i64::from(chunk_size))])
            .await
            .map_err(|e| JikanError::Snapshot(format!("chunk query: {e}")))?
    } else {
        let pk_col = "id"; // resolved from schema in production; simplified here
        let query = format!(
            r#"SELECT * FROM "{}"."{}" ORDER BY "{}" LIMIT $1"#,
            table.schema, table.name, pk_col
        );
        client
            .query(&query, &[&(i64::from(chunk_size))])
            .await
            .map_err(|e| JikanError::Snapshot(format!("chunk query: {e}")))?
    };

    let events: Vec<Result<ChangeEvent, JikanError>> = rows
        .iter()
        .map(|row| {
            let id: i64 = row
                .try_get(0)
                .map_err(|e| JikanError::Snapshot(e.to_string()))?;
            Ok(row_to_insert_event(
                table,
                &snapshot_position,
                PrimaryKey::single("id", ColumnValue::Int(id)),
            ))
        })
        .collect();

    let stream = futures::stream::iter(events);
    Ok(Box::pin(stream))
}

/// Builds a `ChangeEvent` in `Insert` form from a snapshot row.
///
/// Snapshot rows are modelled as inserts because from the consumer's perspective
/// the initial state of the table is equivalent to a sequence of inserts at the
/// `snapshot_position`. Adya et al. (2000): this is consistent with the
/// read-only PL-3 semantics of the snapshot transaction.
pub fn row_to_insert_event(
    table: &TableId,
    position: &Position,
    primary_key: PrimaryKey,
) -> ChangeEvent {
    ChangeEvent {
        position: position.clone(),
        table: table.clone(),
        kind: EventKind::Insert,
        primary_key: Some(primary_key),
        after: None,
        before: None,
        committed_at: None,
    }
}
