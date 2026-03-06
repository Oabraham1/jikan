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
use std::collections::BTreeMap;
use tokio_postgres::{types::Type, NoTls, Row};
use tracing::instrument;

/// Queries the schemas of all tables visible in the publication.
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
            SELECT
                n.nspname                                        AS schema,
                c.relname                                        AS table_name,
                array_agg(a.attname ORDER BY a.attnum)           AS columns,
                array_agg(a.attname ORDER BY a.attnum)
                    FILTER (WHERE EXISTS (
                        SELECT 1 FROM pg_index i
                        WHERE i.indrelid = c.oid
                          AND i.indisprimary
                          AND a.attnum = ANY(i.indkey)
                    ))                                           AS pk_columns
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
                               AND a.attnum > 0
                               AND NOT a.attisdropped
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            GROUP BY n.nspname, c.relname
            "#,
            &[],
        )
        .await
        .map_err(|e| JikanError::Snapshot(format!("schema query: {e}")))?;

    rows.iter()
        .map(|row| {
            let schema: &str = row
                .try_get("schema")
                .map_err(|e| JikanError::Snapshot(e.to_string()))?;
            let name: &str = row
                .try_get("table_name")
                .map_err(|e| JikanError::Snapshot(e.to_string()))?;
            let columns: Vec<String> = row
                .try_get("columns")
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
        .collect()
}

/// Reads one chunk of rows from the table described by `cursor`.
///
/// Rows are returned in primary-key order and fully decoded into
/// `ChangeEvent`s with all columns populated in `after`. The snapshot
/// transaction runs under REPEATABLE READ (Berenson et al. 1995).
#[instrument(skip(connection_string, cursor, schema))]
pub async fn read_chunk(
    connection_string: &str,
    cursor: &ChunkCursor,
    chunk_size: u32,
    schema: &TableSchema,
) -> Result<SnapshotChunkStream, JikanError> {
    if schema.primary_key_columns.is_empty() {
        return Err(JikanError::Snapshot(format!(
            "table {} has no primary key; cannot snapshot without a stable cursor",
            cursor.table
        )));
    }

    let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "snapshot chunk connection failed");
        }
    });

    client
        .execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ", &[])
        .await
        .map_err(|e| JikanError::Snapshot(format!("begin transaction: {e}")))?;

    let pk_col = &schema.primary_key_columns[0];
    let snapshot_position = cursor.snapshot_position.clone();
    let table = &cursor.table;

    let rows = if let Some(last_pk) = &cursor.last_pk {
        let last_val = last_pk.0.get(pk_col).cloned().unwrap_or(ColumnValue::Null);
        let param: Box<dyn tokio_postgres::types::ToSql + Sync + Send> = match &last_val {
            ColumnValue::Int(n) => Box::new(*n),
            ColumnValue::Text(s) => Box::new(s.clone()),
            other => {
                return Err(JikanError::Snapshot(format!(
                    "unsupported PK type for keyset pagination: {other:?}"
                )))
            }
        };
        let query = format!(
            r#"SELECT * FROM "{}"."{}" WHERE "{}" > $1 ORDER BY "{}" LIMIT $2"#,
            table.schema, table.name, pk_col, pk_col
        );
        client
            .query(&query, &[param.as_ref(), &(chunk_size as i64)])
            .await
            .map_err(|e| JikanError::Snapshot(format!("chunk query: {e}")))?
    } else {
        let query = format!(
            r#"SELECT * FROM "{}"."{}" ORDER BY "{}" LIMIT $1"#,
            table.schema, table.name, pk_col
        );
        client
            .query(&query, &[&(chunk_size as i64)])
            .await
            .map_err(|e| JikanError::Snapshot(format!("chunk query: {e}")))?
    };

    client
        .execute("COMMIT", &[])
        .await
        .map_err(|e| JikanError::Snapshot(format!("commit transaction: {e}")))?;

    let events: Vec<Result<ChangeEvent, JikanError>> = rows
        .iter()
        .map(|row| row_to_event(row, table, &snapshot_position, schema))
        .collect();

    Ok(Box::pin(futures::stream::iter(events)))
}

/// Converts a `tokio_postgres::Row` into a `ChangeEvent`.
fn row_to_event(
    row: &Row,
    table: &TableId,
    position: &Position,
    schema: &TableSchema,
) -> Result<ChangeEvent, JikanError> {
    let mut after = BTreeMap::new();

    for col_name in &schema.columns {
        let col = row.columns().iter().find(|c| c.name() == col_name);
        let value = match col {
            None => ColumnValue::Null,
            Some(c) => decode_pg_value(row, c)?,
        };
        after.insert(col_name.clone(), value);
    }

    let mut pk_map = BTreeMap::new();
    for pk_col in &schema.primary_key_columns {
        let val = after.get(pk_col).cloned().unwrap_or(ColumnValue::Null);
        pk_map.insert(pk_col.clone(), val);
    }

    Ok(ChangeEvent {
        position: position.clone(),
        table: table.clone(),
        kind: EventKind::Insert,
        primary_key: Some(PrimaryKey(pk_map)),
        after: Some(after),
        before: None,
        committed_at: None,
    })
}

/// Decodes a single column value from a `tokio_postgres::Row`.
fn decode_pg_value(row: &Row, col: &tokio_postgres::Column) -> Result<ColumnValue, JikanError> {
    let name = col.name();
    match *col.type_() {
        Type::BOOL => {
            let v: Option<bool> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read bool col {name}: {e}")))?;
            Ok(v.map(ColumnValue::Bool).unwrap_or(ColumnValue::Null))
        }
        Type::INT2 => {
            let v: Option<i16> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read int2 col {name}: {e}")))?;
            Ok(v.map(|n| ColumnValue::Int(n as i64))
                .unwrap_or(ColumnValue::Null))
        }
        Type::INT4 | Type::OID => {
            let v: Option<i32> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read int4 col {name}: {e}")))?;
            Ok(v.map(|n| ColumnValue::Int(n as i64))
                .unwrap_or(ColumnValue::Null))
        }
        Type::INT8 => {
            let v: Option<i64> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read int8 col {name}: {e}")))?;
            Ok(v.map(ColumnValue::Int).unwrap_or(ColumnValue::Null))
        }
        Type::FLOAT4 => {
            let v: Option<f32> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read float4 col {name}: {e}")))?;
            Ok(v.map(|f| ColumnValue::Float(f as f64))
                .unwrap_or(ColumnValue::Null))
        }
        Type::FLOAT8 => {
            let v: Option<f64> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read float8 col {name}: {e}")))?;
            Ok(v.map(ColumnValue::Float).unwrap_or(ColumnValue::Null))
        }
        Type::BYTEA => {
            let v: Option<Vec<u8>> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read bytea col {name}: {e}")))?;
            Ok(v.map(ColumnValue::Bytes).unwrap_or(ColumnValue::Null))
        }
        _ => {
            // Fall back to text representation for all other types
            // (uuid, numeric, timestamp, jsonb, arrays, etc.).
            let v: Option<String> = row
                .try_get(name)
                .map_err(|e| JikanError::Snapshot(format!("read text col {name}: {e}")))?;
            Ok(v.map(ColumnValue::Text).unwrap_or(ColumnValue::Null))
        }
    }
}
