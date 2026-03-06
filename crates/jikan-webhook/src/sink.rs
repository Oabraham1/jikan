// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! At-least-once HTTP webhook sink.

use async_trait::async_trait;
use jikan_core::{
    error::JikanError,
    event::ChangeEvent,
    position::Position,
    sink::{DeliveryGuarantee, Sink},
};
use reqwest::Client;
use tracing::instrument;

/// Configuration for the webhook sink.
#[derive(Debug, Clone)]
pub struct WebhookConfig {
    /// The URL to POST events to.
    pub url: String,
    /// Maximum number of events per batch POST.
    pub batch_size: usize,
    /// How many times to retry a failed POST before returning an error.
    pub max_retries: u32,
}

/// An at-least-once sink that POSTs batches of events to an HTTP endpoint.
pub struct WebhookSink {
    client: Client,
    config: WebhookConfig,
    pending: Vec<ChangeEvent>,
}

impl WebhookSink {
    /// Creates a new `WebhookSink`.
    pub fn new(config: WebhookConfig) -> Self {
        Self {
            client: Client::new(),
            config,
            pending: Vec::new(),
        }
    }

    /// Posts a batch of events to the configured endpoint, retrying on failure.
    async fn post_batch(&self, events: &[ChangeEvent]) -> Result<(), JikanError> {
        let mut last_err = None;
        for attempt in 0..=self.config.max_retries {
            match self.client.post(&self.config.url).json(events).send().await {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                Ok(resp) => {
                    let status = resp.status();
                    tracing::warn!(
                        attempt = attempt,
                        status = status.as_u16(),
                        "webhook returned non-success status"
                    );
                    last_err = Some(JikanError::SinkDelivery(format!(
                        "webhook returned {status}"
                    )));
                }
                Err(e) => {
                    tracing::warn!(attempt = attempt, error = %e, "webhook POST failed");
                    last_err = Some(JikanError::SinkDelivery(format!("webhook POST: {e}")));
                }
            }
            if attempt < self.config.max_retries {
                let backoff = std::time::Duration::from_millis(100 * (1 << attempt));
                tokio::time::sleep(backoff).await;
            }
        }
        Err(last_err.unwrap_or_else(|| JikanError::SinkDelivery("webhook: unknown error".into())))
    }
}

#[async_trait]
impl Sink for WebhookSink {
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::AtLeastOnce
    }

    async fn begin(&mut self) -> Result<(), JikanError> {
        self.pending.clear();
        Ok(())
    }

    async fn send(&mut self, event: ChangeEvent) -> Result<(), JikanError> {
        self.pending.push(event);
        Ok(())
    }

    #[instrument(skip(self), fields(position = ?up_to_position))]
    async fn commit(&mut self, up_to_position: &Position) -> Result<(), JikanError> {
        for chunk in self.pending.chunks(self.config.batch_size) {
            self.post_batch(chunk).await?;
        }
        self.pending.clear();
        tracing::debug!(position = ?up_to_position, "webhook batch committed");
        Ok(())
    }

    async fn rollback(&mut self) -> Result<(), JikanError> {
        self.pending.clear();
        Ok(())
    }
}
