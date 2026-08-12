//! Iroh-backed implementation of the transport-neutral delivery stream.
//!
//! This adapter deliberately exposes only the already-opened stream halves and
//! their lifecycle operations. It does not own a [`PeerSession`], connection,
//! delivery queue, or contact allowlist: those concerns belong to the session
//! and delivery-policy layers, which also own deadlines and error mapping.

use std::{future::Future, io, pin::Pin};

use iroh::endpoint::{RecvStream, SendStream};

use super::delivery::DeliveryStream;

/// Thin adapter exposing an Iroh bidirectional stream to delivery policy.
///
/// `send` and `recv` are the independent I/O halves supplied by Iroh. The
/// numeric stream identifier is captured during construction so observability
/// does not need to retain or inspect Iroh stream state. `finished` prevents
/// normal send-side finalisation from being invoked twice, while
/// `reset_requested` makes best-effort abort cleanup idempotent.
///
/// The adapter intentionally contains no peer-session, connection, queue, or
/// allowlist state. Its lifecycle is limited to one already-opened stream and
/// therefore cannot make ownership or policy decisions for its caller.
pub(super) struct IrohDeliveryStream {
    /// Iroh send half used directly by framed delivery writes.
    send: SendStream,
    /// Iroh receive half used directly by framed delivery reads.
    recv: RecvStream,
    /// Stable numeric identifier captured from [`SendStream::id`].
    stream_id: u64,
    /// Whether the local send direction has already been finished.
    finished: bool,
    /// Whether reset cleanup has already been requested.
    reset_requested: bool,
}

impl IrohDeliveryStream {
    /// Creates an adapter around an already-opened Iroh bidirectional stream.
    ///
    /// The stream identifier is read exactly once from `send`; subsequent
    /// delivery stages use the stored numeric value without querying Iroh.
    pub(super) fn new(send: SendStream, recv: RecvStream) -> Self {
        let stream_id = u64::from(send.id());
        Self {
            send,
            recv,
            stream_id,
            finished: false,
            reset_requested: false,
        }
    }
}

impl DeliveryStream for IrohDeliveryStream {
    type Sender = SendStream;
    type Receiver = RecvStream;

    fn stream_id(&self) -> u64 {
        self.stream_id
    }

    fn sender(&mut self) -> &mut Self::Sender {
        &mut self.send
    }

    fn receiver(&mut self) -> &mut Self::Receiver {
        &mut self.recv
    }

    fn finish(&mut self) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + '_>> {
        if self.finished {
            return Box::pin(async { Ok(()) });
        }
        self.finished = true;
        let result = self
            .send
            .finish()
            .map_err(|error| io::Error::other(error.to_string()));
        Box::pin(async move { result })
    }

    fn reset(&mut self) {
        if self.reset_requested {
            return;
        }
        self.reset_requested = true;
        let _ = self.send.reset(0u32.into());
        let _ = self.recv.stop(0u32.into());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies the adapter satisfies the live delivery-stream contract
    /// without requiring an endpoint to construct concrete Iroh stream halves.
    fn assert_delivery_stream<T: DeliveryStream>() {}

    #[test]
    fn iroh_delivery_stream_implements_delivery_stream() {
        assert_delivery_stream::<IrohDeliveryStream>();
    }
}
