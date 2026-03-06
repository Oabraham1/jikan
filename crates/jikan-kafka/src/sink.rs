// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Exactly-once Kafka sink using producer transactions.

use async_trait::async_trait;
use jikan_core::{
    error::JikanError,
    event::ChangeEvent,
    position::Position,
    sink::{DeliveryGuarantee, Sink},
};
use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord, Producer},
};
use tracing::{instrument, warn};

/// Configuration for the Kafka sink.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    /// Comma-separated list of broker addresses.
    pub brokers: String,
    /// The topic to write events to.
    pub topic: String,
    /// A unique transactional ID for exactly-once semantics.
    ///
    /// Carbone et al. (2015): the transactional ID must be stable across
    /// restarts. If two instances use the same ID, one will fence the other.
    pub transactional_id: String,
}

/// An exactly-once Kafka sink backed by producer transactions.
pub struct KafkaSink {
    producer: FutureProducer,
    config: KafkaConfig,
    /// Events accumulated in the current transaction, not yet committed.
    pending: Vec<ChangeEvent>,
}

impl KafkaSink {
    /// Constructs a `KafkaSink` and initialises the transactional producer.
    pub fn new(config: KafkaConfig) -> Result<Self, JikanError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers)
            .set("transactional.id", &config.transactional_id)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .create()
            .map_err(|e| JikanError::SinkDelivery(format!("kafka producer init: {e}")))?;

        producer
            .init_transactions(std::time::Duration::from_secs(10))
            .map_err(|e| JikanError::SinkDelivery(format!("init transactions: {e}")))?;

        Ok(Self {
            producer,
            config,
            pending: Vec::new(),
        })
    }
}

#[async_trait]
impl Sink for KafkaSink {
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::ExactlyOnce
    }

    #[instrument(skip(self))]
    async fn begin(&mut self) -> Result<(), JikanError> {
        self.pending.clear();
        self.producer
            .begin_transaction()
            .map_err(|e| JikanError::SinkDelivery(format!("begin transaction: {e}")))
    }

    async fn send(&mut self, event: ChangeEvent) -> Result<(), JikanError> {
        self.pending.push(event);
        Ok(())
    }

    #[instrument(skip(self), fields(position = ?up_to_position))]
    async fn commit(&mut self, up_to_position: &Position) -> Result<(), JikanError> {
        for event in self.pending.drain(..) {
            let payload = serde_json::to_vec(&event)
                .map_err(|e| JikanError::SinkDelivery(format!("serialise event: {e}")))?;

            let partition_key = format!("{}.{}", event.table.schema, event.table.name);

            let record = FutureRecord::to(&self.config.topic)
                .payload(&payload)
                .key(&partition_key);

            self.producer
                .send(record, std::time::Duration::from_secs(5))
                .await
                .map_err(|(e, _)| JikanError::SinkDelivery(format!("kafka send: {e}")))?;
        }

        self.producer
            .commit_transaction(std::time::Duration::from_secs(10))
            .map_err(|e| JikanError::SinkDelivery(format!("commit transaction: {e}")))?;

        tracing::debug!(position = ?up_to_position, "kafka transaction committed");
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), JikanError> {
        self.pending.clear();
        self.producer
            .abort_transaction(std::time::Duration::from_secs(10))
            .map_err(|e| {
                warn!(error = %e, "kafka abort_transaction failed; producer may need to be recreated");
                JikanError::SinkDelivery(format!("abort transaction: {e}"))
            })
    }
}
