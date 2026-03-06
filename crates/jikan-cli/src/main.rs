// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Jikan command-line entry point.
//!
//! Parses configuration, initialises the tracing subscriber, and wires
//! the selected source connector to the selected sink via the snapshot
//! engine.

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod config;
mod run;
mod validate;

use config::JikanConfig;

/// Jikan — open-source Change Data Capture with exactly-once semantics.
#[derive(Parser)]
#[command(version, about)]
struct Cli {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "jikan.toml")]
    config: std::path::PathBuf,

    /// Override the log level (e.g. `debug`, `info`, `warn`).
    #[arg(long, env = "JIKAN_LOG")]
    log: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let filter = cli.log.as_deref().unwrap_or("info");
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(filter))
        .init();

    let config_bytes = tokio::fs::read(&cli.config)
        .await
        .map_err(|e| anyhow::anyhow!("could not read {}: {e}", cli.config.display()))?;

    let config: JikanConfig = toml::from_str(
        std::str::from_utf8(&config_bytes)
            .map_err(|e| anyhow::anyhow!("config file is not valid UTF-8: {e}"))?,
    )
    .map_err(|e| anyhow::anyhow!("config parse error: {e}"))?;

    validate::validate(&config)?;

    run::run(config).await
}
