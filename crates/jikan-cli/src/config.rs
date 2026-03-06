// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Configuration types deserialised from the TOML file.

use serde::Deserialize;

/// The root configuration for a Jikan pipeline instance.
#[derive(Debug, Deserialize)]
pub struct JikanConfig {
    /// The source database to capture from.
    pub source: SourceConfig,
    /// The sink to deliver events to.
    pub sink: SinkConfig,
    /// Snapshot and checkpoint settings.
    #[serde(default)]
    pub snapshot: SnapshotConfig,
}

/// Source database selection and connection parameters.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SourceConfig {
    /// PostgreSQL via pgoutput logical replication.
    Postgres(PostgresSourceConfig),
    /// MySQL via GTID binlog.
    Mysql(MysqlSourceConfig),
    /// MongoDB via change streams.
    Mongo(MongoSourceConfig),
}

/// PostgreSQL-specific source configuration.
#[derive(Debug, Deserialize)]
pub struct PostgresSourceConfig {
    /// PostgreSQL connection string.
    pub connection_string: String,
    /// The pgoutput publication name.
    pub publication: String,
    /// A stable, unique name for this source instance.
    pub source_id: String,
}

/// MySQL-specific source configuration.
#[derive(Debug, Deserialize)]
pub struct MysqlSourceConfig {
    /// `mysql://` connection URL.
    pub url: String,
    /// A stable, unique name for this source instance.
    pub source_id: String,
    /// The numeric server ID to use for the replica connection.
    pub server_id: u32,
    /// Databases to capture. An empty list captures all databases.
    #[serde(default)]
    pub databases: Vec<String>,
}

/// MongoDB-specific source configuration.
#[derive(Debug, Deserialize)]
pub struct MongoSourceConfig {
    /// `mongodb://` connection URI.
    pub uri: String,
    /// A stable, unique name for this source instance.
    pub source_id: String,
    /// Databases to capture. An empty list captures all databases.
    #[serde(default)]
    pub databases: Vec<String>,
}

/// Sink selection and connection parameters.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SinkConfig {
    /// Kafka with exactly-once producer transactions.
    Kafka(KafkaSinkConfig),
    /// HTTP webhook with at-least-once delivery.
    Webhook(WebhookSinkConfig),
    /// Stdout as newline-delimited JSON (for development and CI).
    Stdout,
}

/// Kafka-specific sink configuration.
#[derive(Debug, Deserialize)]
pub struct KafkaSinkConfig {
    /// Comma-separated broker addresses.
    pub brokers: String,
    /// The topic to write events to.
    pub topic: String,
    /// A unique transactional ID, stable across restarts.
    pub transactional_id: String,
}

/// Webhook-specific sink configuration.
#[derive(Debug, Deserialize)]
pub struct WebhookSinkConfig {
    /// The URL to POST event batches to.
    pub url: String,
    /// Maximum events per POST.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum retry attempts per batch.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

fn default_batch_size() -> usize {
    100
}

fn default_max_retries() -> u32 {
    3
}

/// Snapshot and checkpoint configuration.
#[derive(Debug, Deserialize)]
pub struct SnapshotConfig {
    /// Number of rows per snapshot chunk.
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u32,
    /// Path where the checkpoint file is written.
    #[serde(default = "default_checkpoint_path")]
    pub checkpoint_path: std::path::PathBuf,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            chunk_size: default_chunk_size(),
            checkpoint_path: default_checkpoint_path(),
        }
    }
}

fn default_chunk_size() -> u32 {
    1000
}

fn default_checkpoint_path() -> std::path::PathBuf {
    std::path::PathBuf::from("jikan.checkpoint.json")
}
