// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The top-level PostgreSQL source connector.

use async_trait::async_trait;
use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, RawEvent},
    position::{Lsn, Position, SourceId},
    source::{EventStream, SnapshotChunkStream, Source},
    table::TableSchema,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::NoTls;
use tracing::{info, instrument};

use crate::codec::RelationCache;

/// Configuration for a PostgreSQL source connection.
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// `host=... port=... dbname=... user=... password=...` connection string.
    pub connection_string: String,
    /// A stable identifier for this PostgreSQL instance.
    ///
    /// Used to name the replication slot and disambiguate checkpoints.
    pub source_id: SourceId,
    /// The publication name to subscribe to.
    ///
    /// The publication must already exist: `CREATE PUBLICATION jikan FOR ALL TABLES;`
    pub publication: String,
}

/// A connected PostgreSQL source that can stream replication events and
/// read snapshot chunks.
pub struct PostgresSource {
    config: PostgresConfig,
    /// Cached table schemas, populated by `table_schemas()` and used by
    /// `snapshot_chunk()` to resolve primary keys and column types.
    schema_cache: Arc<RwLock<Vec<TableSchema>>>,
    /// Shared relation cache used by the pgoutput decoder to resolve
    /// RELATION OIDs to column descriptors.
    relation_cache: RelationCache,
}

impl PostgresSource {
    /// Connects to the PostgreSQL instance described by `config`.
    ///
    /// This does not open the replication slot or snapshot transaction — those
    /// are opened lazily when `open_stream` and `snapshot_chunk` are called.
    pub async fn connect(config: PostgresConfig) -> Result<Self, JikanError> {
        // Verify the connection is reachable before returning.
        let (_client, connection) = tokio_postgres::connect(&config.connection_string, NoTls)
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        // Drive the connection in the background. If it fails, subsequent
        // queries will return errors through the client.
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres connection task failed");
            }
        });

        info!(source_id = %config.source_id.0, "connected to postgres");
        Ok(Self {
            config,
            schema_cache: Arc::new(RwLock::new(Vec::new())),
            relation_cache: RelationCache::new(),
        })
    }

    /// Derives the replication slot name from the source identifier.
    ///
    /// The name is truncated and sanitised to fit PostgreSQL's 63-character
    /// limit for identifiers.
    fn slot_name(&self) -> String {
        let raw = format!("jikan_{}", self.config.source_id.0);
        raw.chars()
            .filter(|c| c.is_alphanumeric() || *c == '_')
            .take(63)
            .collect()
    }
}

#[async_trait]
impl Source for PostgresSource {
    fn source_id(&self) -> &SourceId {
        &self.config.source_id
    }

    #[instrument(skip(self), fields(source_id = %self.config.source_id.0))]
    async fn open_stream(&self, start_position: Position) -> Result<EventStream, JikanError> {
        let lsn = match start_position {
            Position::Lsn(l) => l,
            other => {
                return Err(JikanError::ReplicationProtocol(format!(
                    "postgres source requires an LSN position, got {other:?}"
                )))
            }
        };

        let stream = crate::slot::open_replication_stream(
            &self.config.connection_string,
            &self.slot_name(),
            &self.config.publication,
            lsn,
        )
        .await?;

        Ok(stream)
    }

    fn decode(&self, raw: RawEvent) -> Result<Option<ChangeEvent>, JikanError> {
        crate::codec::decode_pgoutput(raw, &self.relation_cache)
    }

    #[instrument(skip(self), fields(source_id = %self.config.source_id.0))]
    async fn current_position(&self) -> Result<Position, JikanError> {
        let (client, connection) = tokio_postgres::connect(&self.config.connection_string, NoTls)
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "postgres connection task failed");
            }
        });

        let row = client
            .query_one("SELECT pg_current_wal_lsn()::text", &[])
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        let lsn_str: &str = row
            .try_get(0)
            .map_err(|e| JikanError::ReplicationProtocol(e.to_string()))?;

        parse_lsn(lsn_str)
    }

    async fn table_schemas(&self) -> Result<Vec<TableSchema>, JikanError> {
        let schemas = crate::snapshot::query_table_schemas(&self.config.connection_string).await?;
        *self.schema_cache.write().await = schemas.clone();
        Ok(schemas)
    }

    async fn snapshot_chunk(
        &self,
        cursor: &ChunkCursor,
        chunk_size: u32,
    ) -> Result<SnapshotChunkStream, JikanError> {
        let cache = self.schema_cache.read().await;
        let schema = cache
            .iter()
            .find(|s| s.table == cursor.table)
            .ok_or_else(|| {
                JikanError::Snapshot(format!(
                    "no schema cached for {}; call table_schemas() first",
                    cursor.table
                ))
            })?;
        crate::snapshot::read_chunk(&self.config.connection_string, cursor, chunk_size, schema)
            .await
    }

    async fn acknowledge(&self, position: &Position) -> Result<(), JikanError> {
        let lsn = match position {
            Position::Lsn(l) => *l,
            other => {
                return Err(JikanError::ReplicationProtocol(format!(
                    "acknowledge requires an LSN, got {other:?}"
                )))
            }
        };
        tracing::debug!(lsn = lsn.0, "acknowledging postgres LSN");
        // Standby status updates are sent through the replication connection,
        // which is managed by the slot module. This call is a no-op here and
        // is handled in the stream loop in slot.rs.
        Ok(())
    }
}

/// Parses a PostgreSQL LSN string of the form `"XXXXXXXX/XXXXXXXX"`.
pub(crate) fn parse_lsn(s: &str) -> Result<Position, JikanError> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| JikanError::ReplicationProtocol(format!("malformed LSN: {s}")))?;
    let hi = u64::from_str_radix(hi.trim(), 16)
        .map_err(|e| JikanError::ReplicationProtocol(format!("malformed LSN high: {e}")))?;
    let lo = u64::from_str_radix(lo.trim(), 16)
        .map_err(|e| JikanError::ReplicationProtocol(format!("malformed LSN low: {e}")))?;
    Ok(Position::Lsn(Lsn((hi << 32) | lo)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_lsn_roundtrip() {
        let pos = parse_lsn("0/1A2B3C4D").expect("valid LSN");
        match pos {
            Position::Lsn(Lsn(n)) => assert_eq!(n, 0x1A2B3C4D),
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn parse_lsn_rejects_garbage() {
        assert!(parse_lsn("not-an-lsn").is_err());
    }
}
