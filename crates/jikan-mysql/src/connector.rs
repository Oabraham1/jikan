// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The top-level MySQL source connector.

use async_trait::async_trait;
use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, RawEvent},
    position::{GtidSet, Position, SourceId},
    source::{EventStream, SnapshotChunkStream, Source},
    table::TableSchema,
};
use tracing::{info, instrument};

/// Configuration for a MySQL source connection.
#[derive(Debug, Clone)]
pub struct MySqlConfig {
    /// `mysql://user:password@host:port/dbname` connection URL.
    pub url: String,
    /// A stable identifier for this MySQL instance.
    pub source_id: SourceId,
    /// The numeric server ID to use for the binlog replica connection.
    ///
    /// Must be unique within the MySQL replication topology.
    pub server_id: u32,
    /// Databases to include in the capture. An empty list captures all databases.
    pub databases: Vec<String>,
}

/// A connected MySQL source that can stream binlog events and read snapshot chunks.
pub struct MySqlSource {
    config: MySqlConfig,
}

impl MySqlSource {
    /// Connects to the MySQL instance described by `config` and verifies
    /// that GTID mode is enabled.
    pub async fn connect(config: MySqlConfig) -> Result<Self, JikanError> {
        let pool = mysql_async::Pool::new(config.url.as_str());
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        verify_gtid_mode(&mut conn).await?;

        info!(source_id = %config.source_id.0, "connected to mysql");
        drop(conn);
        pool.disconnect()
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        Ok(Self { config })
    }
}

/// Checks that the MySQL server has GTID mode enabled.
async fn verify_gtid_mode(conn: &mut mysql_async::Conn) -> Result<(), JikanError> {
    use mysql_async::prelude::Queryable;

    let rows: Vec<mysql_async::Row> = conn
        .query("SHOW VARIABLES LIKE 'gtid_mode'")
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    let mode: Option<String> = rows.first().and_then(|row| row.get("Value"));

    match mode.as_deref() {
        Some("ON") | Some("ON_PERMISSIVE") => Ok(()),
        Some(other) => Err(JikanError::SourceConnection(format!(
            "GTID mode must be ON, got {other}. Set gtid_mode=ON in my.cnf."
        ))),
        None => Err(JikanError::SourceConnection(
            "could not read gtid_mode variable".into(),
        )),
    }
}

#[async_trait]
impl Source for MySqlSource {
    fn source_id(&self) -> &SourceId {
        &self.config.source_id
    }

    #[instrument(skip(self), fields(source_id = %self.config.source_id.0))]
    async fn open_stream(&self, start_position: Position) -> Result<EventStream, JikanError> {
        let gtid_set = match start_position {
            Position::Gtid(g) => g,
            other => {
                return Err(JikanError::ReplicationProtocol(format!(
                    "mysql source requires a GTID position, got {:?}",
                    other
                )))
            }
        };

        crate::binlog::open_binlog_stream(&self.config.url, self.config.server_id, gtid_set).await
    }

    fn decode(&self, raw: RawEvent) -> Result<ChangeEvent, JikanError> {
        crate::codec::decode_binlog_event(raw)
    }

    async fn current_position(&self) -> Result<Position, JikanError> {
        let pool = mysql_async::Pool::new(self.config.url.as_str());
        let mut conn = pool
            .get_conn()
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        let gtid = query_executed_gtid_set(&mut conn).await?;

        pool.disconnect()
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;
        Ok(Position::Gtid(gtid))
    }

    async fn table_schemas(&self) -> Result<Vec<TableSchema>, JikanError> {
        crate::snapshot::query_table_schemas(&self.config.url, &self.config.databases).await
    }

    async fn snapshot_chunk(
        &self,
        cursor: &ChunkCursor,
        chunk_size: u32,
    ) -> Result<SnapshotChunkStream, JikanError> {
        crate::snapshot::read_chunk(&self.config.url, cursor, chunk_size).await
    }

    async fn acknowledge(&self, _position: &Position) -> Result<(), JikanError> {
        // MySQL binlog does not require explicit acknowledgement from the replica.
        // Position tracking is handled by persisting the GTID set in the checkpoint.
        Ok(())
    }
}

/// Queries the `gtid_executed` variable from the MySQL server.
pub(crate) async fn query_executed_gtid_set(
    conn: &mut mysql_async::Conn,
) -> Result<GtidSet, JikanError> {
    use mysql_async::prelude::Queryable;

    let rows: Vec<mysql_async::Row> = conn
        .query("SHOW VARIABLES LIKE 'gtid_executed'")
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    let gtid_str: String = rows
        .first()
        .and_then(|row| row.get("Value"))
        .unwrap_or_default();

    Ok(GtidSet(gtid_str))
}
