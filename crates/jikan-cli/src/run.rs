// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Pipeline wiring: connects the configured source to the configured sink.

use anyhow::Result;
use jikan_core::position::SourceId;
use jikan_snapshot::{SnapshotConfig, SnapshotEngine};
use tracing::info;

use crate::config::{JikanConfig, SinkConfig, SourceConfig};

/// Wires the source and sink from `config` and runs the pipeline.
pub async fn run(config: JikanConfig) -> Result<()> {
    let snapshot_cfg = SnapshotConfig {
        chunk_size: config.snapshot.chunk_size,
        checkpoint_path: config.snapshot.checkpoint_path.clone(),
    };

    match (&config.source, &config.sink) {
        (SourceConfig::Postgres(pg), SinkConfig::Stdout) => {
            use jikan_core::StdoutSink;
            use jikan_postgres::{PostgresConfig, PostgresSource};

            info!(source_id = %pg.source_id, "connecting to postgres");
            let source = PostgresSource::connect(PostgresConfig {
                connection_string: pg.connection_string.clone(),
                source_id: SourceId(pg.source_id.clone()),
                publication: pg.publication.clone(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;

            let sink = StdoutSink::new();
            let mut engine = SnapshotEngine::new(source, sink, snapshot_cfg);
            engine
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
        }

        (SourceConfig::Postgres(pg), SinkConfig::Kafka(k)) => {
            use jikan_kafka::{KafkaConfig, KafkaSink};
            use jikan_postgres::{PostgresConfig, PostgresSource};

            info!(source_id = %pg.source_id, "connecting to postgres with kafka sink");
            let source = PostgresSource::connect(PostgresConfig {
                connection_string: pg.connection_string.clone(),
                source_id: SourceId(pg.source_id.clone()),
                publication: pg.publication.clone(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;

            let sink = KafkaSink::new(KafkaConfig {
                brokers: k.brokers.clone(),
                topic: k.topic.clone(),
                transactional_id: k.transactional_id.clone(),
            })
            .map_err(|e| anyhow::anyhow!("kafka init: {e}"))?;

            let mut engine = SnapshotEngine::new(source, sink, snapshot_cfg);
            engine
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
        }

        (SourceConfig::Mysql(my), SinkConfig::Stdout) => {
            use jikan_core::StdoutSink;
            use jikan_mysql::{MySqlConfig, MySqlSource};

            info!(source_id = %my.source_id, "connecting to mysql");
            let source = MySqlSource::connect(MySqlConfig {
                url: my.url.clone(),
                source_id: SourceId(my.source_id.clone()),
                server_id: my.server_id,
                databases: my.databases.clone(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("mysql connect: {e}"))?;

            let sink = StdoutSink::new();
            let mut engine = SnapshotEngine::new(source, sink, snapshot_cfg);
            engine
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
        }

        (SourceConfig::Postgres(pg), SinkConfig::Webhook(w)) => {
            use jikan_postgres::{PostgresConfig, PostgresSource};
            use jikan_webhook::{WebhookConfig, WebhookSink};

            info!(source_id = %pg.source_id, "connecting to postgres with webhook sink");
            let source = PostgresSource::connect(PostgresConfig {
                connection_string: pg.connection_string.clone(),
                source_id: SourceId(pg.source_id.clone()),
                publication: pg.publication.clone(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("postgres connect: {e}"))?;

            let sink = WebhookSink::new(WebhookConfig {
                url: w.url.clone(),
                batch_size: w.batch_size,
                max_retries: w.max_retries,
            });

            let mut engine = SnapshotEngine::new(source, sink, snapshot_cfg);
            engine
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
        }

        (SourceConfig::Mongo(mg), SinkConfig::Stdout) => {
            use jikan_core::StdoutSink;
            use jikan_mongo::{MongoConfig, MongoSource};

            info!(source_id = %mg.source_id, "connecting to mongodb");
            let source = MongoSource::connect(MongoConfig {
                uri: mg.uri.clone(),
                source_id: SourceId(mg.source_id.clone()),
                databases: mg.databases.clone(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("mongodb connect: {e}"))?;

            let sink = StdoutSink::new();
            let mut engine = SnapshotEngine::new(source, sink, snapshot_cfg);
            engine
                .run()
                .await
                .map_err(|e| anyhow::anyhow!("pipeline error: {e}"))
        }

        (source, sink) => anyhow::bail!(
            "source/sink combination {:?}/{:?} is not yet wired; add it to run.rs",
            std::mem::discriminant(source),
            std::mem::discriminant(sink)
        ),
    }
}
