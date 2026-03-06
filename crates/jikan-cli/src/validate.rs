// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Configuration validation before any I/O is performed.
//!
//! Catching configuration errors early — before the snapshot begins — prevents
//! partial progress that would need to be unwound.

use crate::config::{JikanConfig, SinkConfig, SourceConfig};
use anyhow::{bail, Result};

/// Validates the configuration, returning a descriptive error for the first
/// problem found.
pub fn validate(config: &JikanConfig) -> Result<()> {
    validate_source(&config.source)?;
    validate_sink(&config.sink)?;
    validate_snapshot(&config.snapshot)?;
    Ok(())
}

fn validate_source(source: &SourceConfig) -> Result<()> {
    match source {
        SourceConfig::Postgres(pg) => {
            if pg.connection_string.is_empty() {
                bail!("source.connection_string must not be empty");
            }
            if pg.publication.is_empty() {
                bail!("source.publication must not be empty");
            }
            if pg.source_id.is_empty() {
                bail!("source.source_id must not be empty");
            }
        }
        SourceConfig::Mysql(my) => {
            if my.url.is_empty() {
                bail!("source.url must not be empty");
            }
            if my.source_id.is_empty() {
                bail!("source.source_id must not be empty");
            }
            if my.server_id == 0 {
                bail!(
                    "source.server_id must be a non-zero integer unique within the MySQL topology"
                );
            }
        }
        SourceConfig::Mongo(mg) => {
            if mg.uri.is_empty() {
                bail!("source.uri must not be empty");
            }
            if mg.source_id.is_empty() {
                bail!("source.source_id must not be empty");
            }
        }
    }
    Ok(())
}

fn validate_sink(sink: &SinkConfig) -> Result<()> {
    match sink {
        SinkConfig::Kafka(k) => {
            if k.brokers.is_empty() {
                bail!("sink.brokers must not be empty");
            }
            if k.topic.is_empty() {
                bail!("sink.topic must not be empty");
            }
            if k.transactional_id.is_empty() {
                bail!("sink.transactional_id must not be empty");
            }
        }
        SinkConfig::Webhook(w) => {
            if w.url.is_empty() {
                bail!("sink.url must not be empty");
            }
            if w.batch_size == 0 {
                bail!("sink.batch_size must be greater than zero");
            }
        }
        SinkConfig::Stdout => {}
    }
    Ok(())
}

fn validate_snapshot(snapshot: &crate::config::SnapshotConfig) -> Result<()> {
    if snapshot.chunk_size == 0 {
        bail!("snapshot.chunk_size must be greater than zero");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        JikanConfig, KafkaSinkConfig, PostgresSourceConfig, SinkConfig, SnapshotConfig,
        SourceConfig,
    };

    fn valid_pg_kafka() -> JikanConfig {
        JikanConfig {
            source: SourceConfig::Postgres(PostgresSourceConfig {
                connection_string: "host=localhost dbname=test".into(),
                publication: "jikan_pub".into(),
                source_id: "pg-primary".into(),
            }),
            sink: SinkConfig::Kafka(KafkaSinkConfig {
                brokers: "localhost:9092".into(),
                topic: "jikan.events".into(),
                transactional_id: "jikan-pg-1".into(),
            }),
            snapshot: SnapshotConfig::default(),
        }
    }

    #[test]
    fn valid_config_passes() {
        assert!(validate(&valid_pg_kafka()).is_ok());
    }

    #[test]
    fn empty_source_id_rejected() {
        let mut config = valid_pg_kafka();
        if let SourceConfig::Postgres(ref mut pg) = config.source {
            pg.source_id = String::new();
        }
        assert!(validate(&config).is_err());
    }

    #[test]
    fn empty_kafka_topic_rejected() {
        let mut config = valid_pg_kafka();
        if let SinkConfig::Kafka(ref mut k) = config.sink {
            k.topic = String::new();
        }
        assert!(validate(&config).is_err());
    }

    #[test]
    fn zero_chunk_size_rejected() {
        let mut config = valid_pg_kafka();
        config.snapshot.chunk_size = 0;
        assert!(validate(&config).is_err());
    }
}
