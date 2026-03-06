// Copyright 2026 Ojima Abraham
// SPDX-License-Identifier: Apache-2.0

//! Replication slot lifecycle and logical replication stream.
//!
//! Slot creation and management run over a standard `tokio-postgres` client.
//! The actual streaming protocol (START_REPLICATION) uses the COPY BOTH
//! sub-protocol, which `tokio-postgres` does not natively expose. The
//! `build_raw_event_stream` function encapsulates this gap: once a COPY
//! BOTH–capable transport is available, only that function needs to change.

use jikan_core::{error::JikanError, position::Lsn, source::EventStream};
use tokio_postgres::NoTls;
use tracing::{debug, info, instrument};

/// Opens a logical replication stream from an existing or newly-created slot.
///
/// If the slot does not exist it is created. The stream starts at `start_lsn`,
/// which must be the `snapshot_position` when this is called for the first time
/// in the snapshotting phase (Chandy & Lamport 1985: open the channel before
/// recording the marker).
#[instrument(skip(connection_string))]
pub async fn open_replication_stream(
    connection_string: &str,
    slot_name: &str,
    publication: &str,
    start_lsn: Lsn,
) -> Result<EventStream, JikanError> {
    let replication_dsn = format!("{connection_string} replication=database");

    let (client, connection) = tokio_postgres::connect(&replication_dsn, NoTls)
        .await
        .map_err(|e| JikanError::SourceConnection(format!("replication connect: {e}")))?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            tracing::error!(error = %e, "postgres replication connection failed");
        }
    });

    ensure_slot_exists(&client, slot_name).await?;

    let lsn_str = format!("{:X}/{:X}", start_lsn.0 >> 32, start_lsn.0 & 0xFFFF_FFFF);
    let options = format!(r#"("proto_version" '1', "publication_names" '{publication}')"#);
    let query = format!("START_REPLICATION SLOT {slot_name} LOGICAL {lsn_str} {options}");

    debug!(slot = slot_name, lsn = %lsn_str, "starting logical replication");

    build_raw_event_stream(client, &query).await
}

/// Constructs a pinned stream of raw replication events.
///
/// The PostgreSQL streaming replication protocol uses the COPY BOTH
/// sub-protocol: the server sends CopyData frames containing either
/// XLogData (tag `w`) or PrimaryKeepAlive (tag `k`) messages, while
/// the client may send StandbyStatusUpdate frames back.
///
/// `tokio-postgres` 0.7 does not expose a COPY BOTH API. Integrating
/// a raw-protocol transport (or switching to a fork that supports it)
/// is tracked for a future phase. Until then, this function returns an
/// error describing the gap.
async fn build_raw_event_stream(
    _client: tokio_postgres::Client,
    _query: &str,
) -> Result<EventStream, JikanError> {
    Err(JikanError::ReplicationProtocol(
        "tokio-postgres 0.7 does not support the COPY BOTH sub-protocol \
         required by START_REPLICATION; a COPY BOTH transport is needed"
            .into(),
    ))
}

/// Ensures the named replication slot exists, creating it if necessary.
async fn ensure_slot_exists(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<(), JikanError> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .map_err(|e| JikanError::SourceConnection(e.to_string()))?
        .try_get(0)
        .map_err(|e| JikanError::ReplicationProtocol(e.to_string()))?;

    if !exists {
        client
            .execute(
                &format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput NOEXPORT_SNAPSHOT"),
                &[],
            )
            .await
            .map_err(|e| JikanError::SourceConnection(format!("create replication slot: {e}")))?;
        info!(slot = slot_name, "created replication slot");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;
    use jikan_core::{
        event::RawEvent,
        position::{Lsn, Position},
    };

    /// Parses an XLogData frame from raw copy-data bytes.
    ///
    /// The frame layout is: `w` (1 byte) + WAL start LSN (8 bytes) +
    /// WAL end LSN (8 bytes) + server send timestamp (8 bytes) + pgoutput
    /// payload.
    fn parse_xlog_data(data: &bytes::Bytes) -> Result<Option<RawEvent>, JikanError> {
        if data.is_empty() {
            return Ok(None);
        }

        match data[0] {
            b'w' => {
                const HEADER_LEN: usize = 1 + 8 + 8 + 8;
                if data.len() < HEADER_LEN {
                    return Err(JikanError::ReplicationProtocol(
                        "XLogData frame too short".into(),
                    ));
                }
                let mut cursor = &data[1..];
                let wal_start = cursor.get_u64();

                let payload = data.slice(HEADER_LEN..);
                Ok(Some(RawEvent {
                    position: Position::Lsn(Lsn(wal_start)),
                    payload,
                }))
            }
            b'k' => Ok(None),
            _ => Ok(None),
        }
    }

    #[test]
    fn parse_xlog_data_valid_frame() {
        let mut data = vec![b'w'];
        data.extend_from_slice(&100u64.to_be_bytes());
        data.extend_from_slice(&200u64.to_be_bytes());
        data.extend_from_slice(&300u64.to_be_bytes());
        data.extend_from_slice(b"payload");

        let result = parse_xlog_data(&bytes::Bytes::from(data)).unwrap().unwrap();
        assert_eq!(result.position, Position::Lsn(Lsn(100)));
        assert_eq!(&result.payload[..], b"payload");
    }

    #[test]
    fn parse_xlog_data_keepalive_returns_none() {
        let mut data = vec![b'k'];
        data.extend_from_slice(&[0u8; 17]);
        let result = parse_xlog_data(&bytes::Bytes::from(data)).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn parse_xlog_data_short_frame_is_error() {
        let data = vec![b'w', 0, 0];
        let result = parse_xlog_data(&bytes::Bytes::from(data));
        assert!(result.is_err());
    }
}
