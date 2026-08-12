//! Transport-neutral policy for exchanging one chat message over a bidirectional stream.
//!
//! This module owns message-level framing, receipt validation, allowlist ordering,
//! and stream-reset policy. It deliberately contains no Iroh types: `session` owns
//! connections and adapts an Iroh bidirectional stream to [`DeliveryStream`].

use std::{future::Future, io, pin::Pin};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::mpsc,
    time::{self, Instant},
};

use super::framing::{FrameError, read_single_document, write_document};
use super::{DeliveryCancellation, DeliveryError, DeliveryReceipt, IncomingText, unix_ms_now};
use crate::{
    domain::identity::PeerId,
    protocol::{ChatFrame, MessageId, RejectionCode, WireEnvelope},
};

/// Observable transition reached while exchanging one chat stream.
///
/// Callers use these values for transport-specific observability only. They do not
/// grant ownership of the stream and must not mutate session lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum DeliveryStage {
    /// The exchange has received a bidirectional stream from its owner.
    StreamOpened,
    /// The outbound text document was fully written to the send half.
    TextFrameWritten,
    /// The inbound text document was completely decoded and delimited.
    TextFrameReceived,
    /// A valid outbound receipt was decoded and matched to its message.
    ReceiptReceived,
    /// An inbound accepted or rejected receipt was fully written.
    ReceiptWritten,
}

/// Successful terminal result of handling one inbound text document.
///
/// Rejection is successful protocol completion, rather than a connection error:
/// callers must retain the stream and connection after [`Self::Rejected`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum InboundOutcome {
    /// An authorised text was published before its accepted receipt was sent.
    Accepted {
        /// Identifier supplied by the remote text frame.
        message_id: MessageId,
    },
    /// An unauthorised text was rejected without being published locally.
    Rejected {
        /// Identifier supplied by the remote text frame.
        message_id: MessageId,
        /// Protocol rejection reason written to the remote peer.
        code: RejectionCode,
    },
}

/// Bidirectional stream operations required by the delivery exchange policy.
///
/// The owner supplies independent read and write halves. `finish` closes only the
/// local send direction and returns an owned, `Send` future so adapters can perform
/// runtime-specific finalisation. `reset` is best-effort and callers invoke it once
/// for each failing exchange path; implementations must therefore tolerate repeated
/// calls without panicking or amplifying a reset into a second transport action.
pub(super) trait DeliveryStream {
    /// Concrete writable half supplied by the transport adapter.
    type Sender: AsyncWrite + Unpin + Send;
    /// Concrete readable half supplied by the transport adapter.
    type Receiver: AsyncRead + Unpin + Send;

    /// Returns the transport-local identifier used solely for observability.
    fn stream_id(&self) -> u64;

    /// Borrows the local send half for framed document writes.
    fn sender(&mut self) -> &mut Self::Sender;

    /// Borrows the local receive half for framed document reads.
    fn receiver(&mut self) -> &mut Self::Receiver;

    /// Finishes the local send direction after its single protocol document.
    fn finish(&mut self) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + '_>>;

    /// Best-effort abort of this stream's exchange without closing its connection.
    fn reset(&mut self);
}

/// Runs one fallible operation until its absolute deadline or cancellation wins.
///
/// The nested result preserves the operation's error type: `Ok(Err(error))` is an
/// operation failure, whereas outer `Err(DeliveryError::TimedOut)` denotes deadline
/// expiry or cancellation. Cancellation and successful completion racing at the same
/// instant are resolved by Tokio's unbiased selection and have no side effects here.
pub(super) async fn with_deadline<T, E, F>(
    deadline: Instant,
    operation: F,
    cancellation: &DeliveryCancellation,
) -> Result<Result<T, E>, DeliveryError>
where
    F: Future<Output = Result<T, E>>,
{
    tokio::pin!(operation);
    tokio::select! {
        result = time::timeout_at(deadline, &mut operation) => result.map_err(|_| DeliveryError::TimedOut),
        _ = cancellation.wait() => Err(DeliveryError::TimedOut),
    }
}

/// Exchanges one outbound `Text` document for an authenticated peer receipt.
///
/// The caller owns `stream` for its full lifetime and supplies the common delivery
/// deadline and cancellation token. Every potentially blocking stage (write, finish,
/// and receipt read) shares those bounds. Any framing, timeout, cancellation, EOF,
/// finish, or receipt-validation error resets exactly this stream once and returns a
/// mapped [`DeliveryError`]; success never resets the stream.
pub(super) async fn outbound_exchange<S, Observe>(
    stream: &mut S,
    envelope: WireEnvelope,
    deadline: Instant,
    cancellation: &DeliveryCancellation,
    mut observe: Observe,
) -> Result<DeliveryReceipt, DeliveryError>
where
    S: DeliveryStream,
    Observe: FnMut(DeliveryStage),
{
    let ChatFrame::Text { message_id, .. } = envelope.frame else {
        stream.reset();
        return Err(DeliveryError::ProtocolViolation);
    };
    let _ = stream.stream_id();
    observe(DeliveryStage::StreamOpened);
    if let Err(error) = with_deadline(
        deadline,
        write_document(stream.sender(), &envelope),
        cancellation,
    )
    .await
    .and_then(|result| result.map_err(map_frame_error))
    {
        stream.reset();
        return Err(error);
    }
    observe(DeliveryStage::TextFrameWritten);
    if let Err(error) = finish_with_deadline(stream, deadline, cancellation).await {
        stream.reset();
        return Err(error);
    }
    let receipt = match with_deadline(
        deadline,
        read_single_document(stream.receiver()),
        cancellation,
    )
    .await
    {
        Ok(Ok(envelope)) => validate_receipt(message_id, envelope.frame),
        Ok(Err(error)) => Err(map_frame_error(error)),
        Err(error) => Err(error),
    };
    match receipt {
        Ok(receipt) => {
            observe(DeliveryStage::ReceiptReceived);
            Ok(receipt)
        }
        Err(error) => {
            stream.reset();
            Err(error)
        }
    }
}

/// Receives, authorises, publishes, and acknowledges one inbound `Text` document.
///
/// `authorise` runs only after a complete text frame is decoded, and before any
/// [`IncomingText`] publication. The read, authorisation, publication, receipt
/// write, and finish stages all share the supplied deadline and cancellation token.
/// A false result writes `UnknownContact` and returns [`InboundOutcome::Rejected`]
/// without resetting. Every malformed, timed-out, cancelled, EOF, publication, or
/// stream-write failure resets this stream once; the surrounding connection remains
/// owned by the caller.
pub(super) async fn inbound_exchange<S, Authorise, Authorisation, Observe>(
    stream: &mut S,
    peer_id: PeerId,
    incoming_tx: &mpsc::Sender<IncomingText>,
    deadline: Instant,
    cancellation: &DeliveryCancellation,
    authorise: Authorise,
    mut observe: Observe,
) -> Result<InboundOutcome, DeliveryError>
where
    S: DeliveryStream,
    Authorise: FnOnce() -> Authorisation,
    Authorisation: Future<Output = bool>,
    Observe: FnMut(DeliveryStage),
{
    let envelope = match with_deadline(
        deadline,
        read_single_document(stream.receiver()),
        cancellation,
    )
    .await
    {
        Ok(Ok(envelope)) => envelope,
        Ok(Err(error)) => {
            stream.reset();
            return Err(map_frame_error(error));
        }
        Err(error) => {
            stream.reset();
            return Err(error);
        }
    };
    let ChatFrame::Text {
        message_id,
        sent_at_unix_ms,
        body,
    } = envelope.frame
    else {
        stream.reset();
        return Err(DeliveryError::ProtocolViolation);
    };
    observe(DeliveryStage::TextFrameReceived);

    let authorised = match with_deadline(
        deadline,
        async { Ok::<bool, std::convert::Infallible>(authorise().await) },
        cancellation,
    )
    .await
    {
        Ok(Ok(authorised)) => authorised,
        Ok(Err(never)) => match never {},
        Err(error) => {
            stream.reset();
            return Err(error);
        }
    };
    if !authorised {
        let outcome = InboundOutcome::Rejected {
            message_id,
            code: RejectionCode::UnknownContact,
        };
        let rejected = WireEnvelope::new(ChatFrame::rejected(
            message_id,
            RejectionCode::UnknownContact,
        ));
        if let Err(error) = with_deadline(
            deadline,
            write_document(stream.sender(), &rejected),
            cancellation,
        )
        .await
        .and_then(|result| result.map_err(map_frame_error))
        {
            stream.reset();
            return Err(error);
        }
        if let Err(error) = finish_with_deadline(stream, deadline, cancellation).await {
            stream.reset();
            return Err(error);
        }
        observe(DeliveryStage::ReceiptWritten);
        return Ok(outcome);
    }

    let incoming = IncomingText {
        peer_id,
        message_id,
        sent_at_unix_ms,
        body,
    };
    if let Err(error) = with_deadline(deadline, incoming_tx.send(incoming), cancellation)
        .await
        .and_then(|result| result.map_err(|_| DeliveryError::ShutDown))
    {
        stream.reset();
        return Err(error);
    }
    let accepted = WireEnvelope::new(ChatFrame::accepted(message_id, unix_ms_now()));
    if let Err(error) = with_deadline(
        deadline,
        write_document(stream.sender(), &accepted),
        cancellation,
    )
    .await
    .and_then(|result| result.map_err(map_frame_error))
    {
        stream.reset();
        return Err(error);
    }
    if let Err(error) = finish_with_deadline(stream, deadline, cancellation).await {
        stream.reset();
        return Err(error);
    }
    observe(DeliveryStage::ReceiptWritten);
    Ok(InboundOutcome::Accepted { message_id })
}

/// Finishes a local send direction subject to the exchange's shared bounds.
///
/// The stream owner retains reset ownership: this helper maps an adapter finish
/// error to [`DeliveryError::Transport`] but performs no reset itself, allowing its
/// caller to request exactly one reset on every failing path.
async fn finish_with_deadline<S>(
    stream: &mut S,
    deadline: Instant,
    cancellation: &DeliveryCancellation,
) -> Result<(), DeliveryError>
where
    S: DeliveryStream,
{
    with_deadline(deadline, stream.finish(), cancellation)
        .await
        .and_then(|result| result.map_err(|error| DeliveryError::Transport(error.to_string())))
}

/// Maps framing failures to the delivery-level error taxonomy.
///
/// I/O and declared-length failures are transport failures, while decoded protocol
/// violations and trailing bytes are peer protocol violations.
fn map_frame_error(error: FrameError) -> DeliveryError {
    match error {
        FrameError::Io(error) => DeliveryError::Transport(error.to_string()),
        FrameError::DeclaredFrameTooLarge { declared } => {
            DeliveryError::Transport(format!("declared frame too large: {declared}"))
        }
        FrameError::TrailingData | FrameError::Protocol(_) => DeliveryError::ProtocolViolation,
    }
}

/// Validates that an outbound reply is the receipt for `expected`.
///
/// Only an accepted receipt yields [`DeliveryReceipt`]; a matching rejection maps
/// to its protocol error, and every other frame or identifier fails closed.
fn validate_receipt(
    expected: MessageId,
    frame: ChatFrame,
) -> Result<DeliveryReceipt, DeliveryError> {
    match frame {
        ChatFrame::Accepted {
            message_id,
            received_at_unix_ms,
        } if message_id == expected => Ok(DeliveryReceipt {
            message_id,
            received_at_unix_ms,
        }),
        ChatFrame::Rejected { message_id, code } if message_id == expected => {
            Err(DeliveryError::Rejected(code))
        }
        _ => Err(DeliveryError::ProtocolViolation),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        sync::atomic::{AtomicUsize, Ordering},
        task::Poll,
        time::Duration,
    };

    use tokio::{
        io::{AsyncWriteExt, DuplexStream},
        time::Instant,
    };

    use super::*;
    use crate::network::chat::framing::{read_document, write_document};
    use crate::protocol::{ChatFrame, MessageId, WireEnvelope};

    /// In-memory implementation of [`DeliveryStream`] with independent directions.
    ///
    /// The exchange owns `sender` and `receiver`; tests retain the opposite
    /// [`MemoryPeer`] halves and exercise the real length-prefixed framing protocol.
    /// `finished` records local send finalisation, while `reset_count` proves callers
    /// request at most one reset for a failed exchange.
    struct MemoryStream {
        /// Local write half that delivers bytes to `MemoryPeer::receiver`.
        sender: DuplexStream,
        /// Local read half that receives bytes from `MemoryPeer::sender`.
        receiver: DuplexStream,
        /// Stable test-only stream identifier for stage observability.
        stream_id: u64,
        /// Whether this stream's local send half has been finished.
        finished: bool,
        /// Whether finishing the local send half waits forever for a peer action.
        finish_never: bool,
        /// Count of reset requests, capped at one to model idempotent reset.
        reset_count: Arc<AtomicUsize>,
    }

    /// Remote ends of a [`MemoryStream`] used to drive framed peer behaviour.
    struct MemoryPeer {
        /// Remote write half supplying bytes to `MemoryStream::receiver`.
        sender: DuplexStream,
        /// Remote read half consuming bytes from `MemoryStream::sender`.
        receiver: DuplexStream,
    }

    impl MemoryStream {
        /// Creates a stream and its remote peer with independent duplex directions.
        fn pair(stream_id: u64) -> (Self, MemoryPeer) {
            let (sender, peer_receiver) = tokio::io::duplex(70_000);
            let (peer_sender, receiver) = tokio::io::duplex(70_000);
            (
                Self {
                    sender,
                    receiver,
                    stream_id,
                    finished: false,
                    finish_never: false,
                    reset_count: Arc::new(AtomicUsize::new(0)),
                },
                MemoryPeer {
                    sender: peer_sender,
                    receiver: peer_receiver,
                },
            )
        }

        /// Configures `finish` to remain pending, simulating a stalled transport.
        fn hang_finish(&mut self) {
            self.finish_never = true;
        }
    }

    impl DeliveryStream for MemoryStream {
        type Sender = DuplexStream;
        type Receiver = DuplexStream;

        fn stream_id(&self) -> u64 {
            self.stream_id
        }

        fn sender(&mut self) -> &mut Self::Sender {
            &mut self.sender
        }

        fn receiver(&mut self) -> &mut Self::Receiver {
            &mut self.receiver
        }

        fn finish(&mut self) -> Pin<Box<dyn Future<Output = Result<(), io::Error>> + Send + '_>> {
            self.finished = true;
            if self.finish_never {
                return Box::pin(std::future::pending());
            }
            Box::pin(async move { self.sender.shutdown().await })
        }

        fn reset(&mut self) {
            let _ = self
                .reset_count
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
        }
    }

    #[tokio::test]
    async fn outbound_exchange_validates_matching_receipt() {
        let message_id = MessageId::new([1; 16]);
        let envelope = WireEnvelope::new(ChatFrame::text(message_id, 10, "hello").unwrap());
        let expected_envelope = envelope.clone();
        let (mut stream, mut peer) = MemoryStream::pair(7);
        let responder = tokio::spawn(async move {
            let received = read_document(&mut peer.receiver).await.unwrap();
            assert_eq!(received, expected_envelope);
            write_document(
                &mut peer.sender,
                &WireEnvelope::new(ChatFrame::accepted(message_id, 20)),
            )
            .await
            .unwrap();
            peer.sender.shutdown().await.unwrap();
        });
        let cancellation = DeliveryCancellation::new();
        let mut stages = Vec::new();

        let receipt = outbound_exchange(
            &mut stream,
            envelope,
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            |stage| stages.push(stage),
        )
        .await
        .unwrap();

        responder.await.unwrap();
        assert_eq!(
            receipt,
            DeliveryReceipt {
                message_id,
                received_at_unix_ms: 20,
            }
        );
        assert_eq!(
            stages,
            vec![
                DeliveryStage::StreamOpened,
                DeliveryStage::TextFrameWritten,
                DeliveryStage::ReceiptReceived,
            ]
        );
    }

    #[tokio::test]
    async fn inbound_exchange_publishes_before_accepted_receipt() {
        let message_id = MessageId::new([2; 16]);
        let peer_id = PeerId::from_canonical("authorised-peer".to_owned());
        let (mut stream, mut peer) = MemoryStream::pair(8);
        let (incoming_tx, mut incoming_rx) = mpsc::channel(1);
        incoming_tx
            .send(IncomingText {
                peer_id: PeerId::from_canonical("queued-first".to_owned()),
                message_id: MessageId::new([0; 16]),
                sent_at_unix_ms: 0,
                body: "occupies the queue".to_owned(),
            })
            .await
            .unwrap();
        write_document(
            &mut peer.sender,
            &WireEnvelope::new(ChatFrame::text(message_id, 30, "inbound").unwrap()),
        )
        .await
        .unwrap();
        peer.sender.shutdown().await.unwrap();
        let cancellation = DeliveryCancellation::new();

        let exchange = inbound_exchange(
            &mut stream,
            peer_id.clone(),
            &incoming_tx,
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            || async { true },
            |_| {},
        );
        tokio::pin!(exchange);
        std::future::poll_fn(|context| {
            assert!(exchange.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        let queued = incoming_rx.recv().await.unwrap();
        assert_eq!(queued.body, "occupies the queue");
        let outcome = exchange.await.unwrap();
        let incoming = incoming_rx.recv().await.unwrap();
        let receipt = read_single_document(&mut peer.receiver).await.unwrap();

        assert_eq!(
            incoming,
            IncomingText {
                peer_id,
                message_id,
                sent_at_unix_ms: 30,
                body: "inbound".to_owned(),
            }
        );
        assert_eq!(outcome, InboundOutcome::Accepted { message_id });
        assert!(matches!(
            receipt.frame,
            ChatFrame::Accepted {
                message_id: received_id,
                ..
            } if received_id == message_id
        ));
    }

    #[tokio::test]
    async fn inbound_exchange_rejects_unknown_contact_without_publishing_text() {
        let message_id = MessageId::new([3; 16]);
        let (mut stream, mut peer) = MemoryStream::pair(9);
        let (incoming_tx, mut incoming_rx) = mpsc::channel(1);
        let writer = tokio::spawn(async move {
            write_document(
                &mut peer.sender,
                &WireEnvelope::new(ChatFrame::text(message_id, 40, "blocked").unwrap()),
            )
            .await
            .unwrap();
            peer.sender.shutdown().await.unwrap();
            read_single_document(&mut peer.receiver).await.unwrap()
        });
        let cancellation = DeliveryCancellation::new();

        let outcome = inbound_exchange(
            &mut stream,
            PeerId::from_canonical("removed-peer".to_owned()),
            &incoming_tx,
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            || async { false },
            |_| {},
        )
        .await
        .unwrap();
        let receipt = writer.await.unwrap();

        assert_eq!(
            outcome,
            InboundOutcome::Rejected {
                message_id,
                code: RejectionCode::UnknownContact,
            }
        );
        assert!(incoming_rx.try_recv().is_err());
        assert_eq!(
            receipt,
            WireEnvelope::new(ChatFrame::rejected(
                message_id,
                RejectionCode::UnknownContact
            ))
        );
        assert_eq!(stream.reset_count.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn inbound_exchange_resets_after_trailing_data() {
        let message_id = MessageId::new([4; 16]);
        let (mut stream, mut peer) = MemoryStream::pair(10);
        let (incoming_tx, _incoming_rx) = mpsc::channel(1);
        let writer = tokio::spawn(async move {
            write_document(
                &mut peer.sender,
                &WireEnvelope::new(ChatFrame::text(message_id, 50, "first").unwrap()),
            )
            .await
            .unwrap();
            write_document(
                &mut peer.sender,
                &WireEnvelope::new(ChatFrame::text(MessageId::new([5; 16]), 51, "second").unwrap()),
            )
            .await
            .unwrap();
            peer.sender.shutdown().await.unwrap();
        });
        let cancellation = DeliveryCancellation::new();

        let result = inbound_exchange(
            &mut stream,
            PeerId::from_canonical("malformed-peer".to_owned()),
            &incoming_tx,
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            || async { true },
            |_| {},
        )
        .await;

        writer.await.unwrap();
        assert!(matches!(result, Err(DeliveryError::ProtocolViolation)));
        assert_eq!(stream.reset_count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn with_deadline_maps_expiration_to_timed_out() {
        let cancellation = DeliveryCancellation::new();

        let result = with_deadline(
            Instant::now() - Duration::from_millis(1),
            std::future::pending::<Result<(), ()>>(),
            &cancellation,
        )
        .await;

        assert!(matches!(result, Err(DeliveryError::TimedOut)));
    }

    #[tokio::test]
    async fn with_deadline_maps_cancellation_to_timed_out() {
        let cancellation = DeliveryCancellation::new();
        assert!(cancellation.cancel());

        let result = with_deadline(
            Instant::now() + Duration::from_secs(1),
            std::future::pending::<Result<(), ()>>(),
            &cancellation,
        )
        .await;

        assert!(matches!(result, Err(DeliveryError::TimedOut)));
    }

    #[tokio::test]
    async fn outbound_exchange_cancels_hanging_finish_and_resets_once() {
        let message_id = MessageId::new([6; 16]);
        let (mut stream, _peer) = MemoryStream::pair(11);
        stream.hang_finish();
        let reset_count = stream.reset_count.clone();
        let cancellation = DeliveryCancellation::new();
        let exchange = outbound_exchange(
            &mut stream,
            WireEnvelope::new(ChatFrame::text(message_id, 60, "stalled").unwrap()),
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            |_| {},
        );
        tokio::pin!(exchange);
        std::future::poll_fn(|context| {
            assert!(exchange.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(cancellation.cancel());

        {
            let result = tokio::time::timeout(Duration::from_millis(50), &mut exchange).await;
            assert!(matches!(result, Ok(Err(DeliveryError::TimedOut))));
        }
        assert_eq!(reset_count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn inbound_exchange_cancels_hanging_authorisation_and_resets_once() {
        let message_id = MessageId::new([7; 16]);
        let (mut stream, mut peer) = MemoryStream::pair(12);
        write_document(
            &mut peer.sender,
            &WireEnvelope::new(ChatFrame::text(message_id, 70, "authorise").unwrap()),
        )
        .await
        .unwrap();
        peer.sender.shutdown().await.unwrap();
        let (incoming_tx, _incoming_rx) = mpsc::channel(1);
        let reset_count = stream.reset_count.clone();
        let cancellation = DeliveryCancellation::new();
        let exchange = inbound_exchange(
            &mut stream,
            PeerId::from_canonical("pending-authorisation".to_owned()),
            &incoming_tx,
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            std::future::pending,
            |_| {},
        );
        tokio::pin!(exchange);
        std::future::poll_fn(|context| {
            assert!(exchange.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(cancellation.cancel());

        {
            let result = tokio::time::timeout(Duration::from_millis(50), &mut exchange).await;
            assert!(matches!(result, Ok(Err(DeliveryError::TimedOut))));
        }
        assert_eq!(reset_count.load(Ordering::Acquire), 1);
    }

    #[tokio::test]
    async fn inbound_exchange_cancels_full_publish_queue_and_resets_once() {
        let message_id = MessageId::new([8; 16]);
        let (mut stream, mut peer) = MemoryStream::pair(13);
        write_document(
            &mut peer.sender,
            &WireEnvelope::new(ChatFrame::text(message_id, 80, "publish").unwrap()),
        )
        .await
        .unwrap();
        peer.sender.shutdown().await.unwrap();
        let (incoming_tx, _incoming_rx) = mpsc::channel(1);
        incoming_tx
            .send(IncomingText {
                peer_id: PeerId::from_canonical("queue-holder".to_owned()),
                message_id: MessageId::new([9; 16]),
                sent_at_unix_ms: 0,
                body: "fills queue".to_owned(),
            })
            .await
            .unwrap();
        let reset_count = stream.reset_count.clone();
        let cancellation = DeliveryCancellation::new();
        let exchange = inbound_exchange(
            &mut stream,
            PeerId::from_canonical("queued-peer".to_owned()),
            &incoming_tx,
            Instant::now() + Duration::from_secs(1),
            &cancellation,
            || async { true },
            |_| {},
        );
        tokio::pin!(exchange);
        std::future::poll_fn(|context| {
            assert!(exchange.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        assert!(cancellation.cancel());

        {
            let result = tokio::time::timeout(Duration::from_millis(50), &mut exchange).await;
            assert!(matches!(result, Ok(Err(DeliveryError::TimedOut))));
        }
        assert_eq!(reset_count.load(Ordering::Acquire), 1);
    }
}
