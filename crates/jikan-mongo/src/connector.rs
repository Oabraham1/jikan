// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! The top-level MongoDB source connector.

use async_trait::async_trait;
use jikan_core::{
    checkpoint::ChunkCursor,
    error::JikanError,
    event::{ChangeEvent, RawEvent},
    position::{OplogTimestamp, Position, SourceId},
    source::{EventStream, SnapshotChunkStream, Source},
    table::TableSchema,
};
use mongodb::{bson::doc, options::ClientOptions, Client};
use tracing::{info, instrument};

/// Configuration for a MongoDB source connection.
#[derive(Debug, Clone)]
pub struct MongoConfig {
    /// `mongodb://user:password@host:port/` connection URI.
    pub uri: String,
    /// A stable identifier for this MongoDB replica set.
    pub source_id: SourceId,
    /// The database names to include. An empty list captures all databases
    /// except `admin`, `config`, and `local`.
    pub databases: Vec<String>,
}

/// A connected MongoDB source that can stream change events and read snapshot chunks.
pub struct MongoSource {
    config: MongoConfig,
    client: Client,
}

impl MongoSource {
    /// Connects to the MongoDB cluster described by `config` and verifies
    /// that it is a replica set (change streams require a replica set or
    /// sharded cluster).
    pub async fn connect(config: MongoConfig) -> Result<Self, JikanError> {
        let opts = ClientOptions::parse(&config.uri)
            .await
            .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        let client =
            Client::with_options(opts).map_err(|e| JikanError::SourceConnection(e.to_string()))?;

        verify_replica_set(&client).await?;

        info!(source_id = %config.source_id.0, "connected to mongodb");
        Ok(Self { config, client })
    }
}

async fn verify_replica_set(client: &Client) -> Result<(), JikanError> {
    let result = client
        .database("admin")
        .run_command(doc! { "isMaster": 1 }, None)
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    let is_replica = result.get_str("setName").is_ok()
        || result
            .get_str("msg")
            .map(|m| m == "isdbgrid")
            .unwrap_or(false);

    if !is_replica {
        return Err(JikanError::SourceConnection(
            "change streams require a replica set or mongos; standalone instances are not supported"
                .into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl Source for MongoSource {
    fn source_id(&self) -> &SourceId {
        &self.config.source_id
    }

    #[instrument(skip(self), fields(source_id = %self.config.source_id.0))]
    async fn open_stream(&self, start_position: Position) -> Result<EventStream, JikanError> {
        let ts = match start_position {
            Position::OplogTs(t) => t,
            other => {
                return Err(JikanError::ReplicationProtocol(format!(
                    "mongodb source requires an OplogTs position, got {other:?}"
                )))
            }
        };

        crate::snapshot::open_change_stream(&self.client, &self.config.databases, ts).await
    }

    fn decode(&self, raw: RawEvent) -> Result<ChangeEvent, JikanError> {
        crate::codec::decode_change_event(raw)
    }

    async fn current_position(&self) -> Result<Position, JikanError> {
        query_cluster_time(&self.client).await
    }

    async fn table_schemas(&self) -> Result<Vec<TableSchema>, JikanError> {
        crate::snapshot::query_collection_schemas(&self.client, &self.config.databases).await
    }

    async fn snapshot_chunk(
        &self,
        cursor: &ChunkCursor,
        chunk_size: u32,
    ) -> Result<SnapshotChunkStream, JikanError> {
        crate::snapshot::read_chunk(&self.client, cursor, chunk_size).await
    }

    async fn acknowledge(&self, _position: &Position) -> Result<(), JikanError> {
        // MongoDB change streams do not require explicit acknowledgement.
        // The resume token persisted in the checkpoint serves this purpose.
        Ok(())
    }
}

/// Reads the current cluster time from the `$clusterTime` field returned
/// by any MongoDB command.
async fn query_cluster_time(client: &Client) -> Result<Position, JikanError> {
    let result = client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?;

    // $clusterTime.clusterTime is a BSON Timestamp with (seconds, ordinal).
    let cluster_time = result
        .get_document("$clusterTime")
        .and_then(|ct| ct.get_timestamp("clusterTime"))
        .map_err(|e| JikanError::ReplicationProtocol(format!("no clusterTime in response: {e}")))?;

    Ok(Position::OplogTs(OplogTimestamp {
        seconds: cluster_time.time,
        ordinal: cluster_time.increment,
        term: 0, // term is not exposed by the ping response; 0 is safe here
    }))
}
