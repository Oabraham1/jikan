// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Binlog stream connection and event reading.

use jikan_core::{
    error::JikanError,
    event::RawEvent,
    position::{GtidSet, Position},
    source::EventStream,
};
use tracing::instrument;

/// Opens a binlog replication stream starting after `start_gtid_set`.
///
/// Chandy & Lamport (1985): this must be called before any snapshot rows
/// are read. The binlog stream anchored at the GTID set is the channel
/// whose state must be captured to avoid missing in-flight events.
#[instrument(skip(url))]
pub async fn open_binlog_stream(
    url: &str,
    server_id: u32,
    start_gtid_set: GtidSet,
) -> Result<EventStream, JikanError> {
    // mysql_async does not yet provide a built-in binlog streaming API.
    // Production implementations use the raw COM_BINLOG_DUMP_GTID command.
    // This stub returns an error; the Phase 04 integration test wires in the
    // actual mysql-binlog-connector or a compatible library.
    tracing::debug!(
        server_id = server_id,
        gtid = %start_gtid_set.0,
        "opening mysql binlog stream"
    );
    let _ = url;
    Err(JikanError::ReplicationProtocol(
        "binlog stream is completed in Phase 04 integration".into(),
    ))
}

/// Wraps a raw binlog payload with its GTID-derived position.
pub fn make_raw_event(gtid: &str, payload: bytes::Bytes) -> RawEvent {
    RawEvent {
        position: Position::Gtid(GtidSet(gtid.to_owned())),
        payload,
    }
}
