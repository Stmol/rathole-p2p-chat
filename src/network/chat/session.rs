//! Per-peer session actor, connection ownership, and delivery orchestration.
//!
//! This module is the sole owner of the lifecycle for one authenticated Iroh
//! peer: it serializes outgoing queue admission, selects and drains connections,
//! coordinates exactly-once completion, and publishes existing connection and
//! path diagnostics. Delivery framing, receipt validation, authorization order,
//! and stream reset policy remain in [`super::delivery`]; Iroh stream finish and
//! reset primitives remain in [`super::iroh_stream`]. All actor mutation occurs
//! on its Tokio task. Spawned I/O tasks communicate results through
//! [`SessionControl`], so a malformed stream cannot directly close a healthy
//! connection or publish a spurious disconnected state.

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant as StdInstant,
};

use iroh::{EndpointId, endpoint::Connection};
use n0_future::StreamExt;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, mpsc, watch},
    task::JoinHandle,
    time::Instant,
};

use super::delivery::{
    DeliveryStage, DeliveryStream, inbound_exchange, outbound_exchange, with_deadline,
};
use super::iroh_stream::IrohDeliveryStream;
use super::path_diagnostics::{path_event_details, selected_path_from_connection};
use super::transport::TransportInner;
use super::{
    CompletionSlot, DeliveryCancellation, DeliveryError, DeliveryReceipt, INBOUND_STREAM_TIMEOUT,
    OUTGOING_QUEUE_CAPACITY, PeerConnectionEvent, resolve_once,
};
use crate::domain::{
    connection::{ContactConnectionState, SelectedPath},
    identity::PeerId,
};
use crate::logging::{self, LogFields};
use crate::protocol::{MessageId, WireEnvelope};

/// An accepted outbound command and the bounds governing its eventual exchange.
///
/// The producer transfers ownership to [`PeerSession::try_enqueue`], then the
/// session actor owns the command until it resolves `completion` once. `envelope`
/// is the already-validated wire request, while `message_id` is retained for
/// queue and diagnostic correlation. `deadline` is absolute and includes
/// queueing; `cancellation` is shared with stream I/O to arbitrate races without
/// extending delivery lifetime.
#[derive(Clone)]
pub(super) struct QueuedDelivery {
    /// Validated text envelope consumed by the outbound delivery policy.
    pub(super) envelope: WireEnvelope,
    /// Stable message identity used for completion matching and diagnostics.
    pub(super) message_id: MessageId,
    /// One-shot receiver resolved by the actor with the terminal result.
    pub(super) completion: CompletionSlot,
    /// Shared cancellation latch consulted by queue and stream stages.
    pub(super) cancellation: Arc<DeliveryCancellation>,
    /// Absolute local-delivery bound, including queue wait and network I/O.
    pub(super) deadline: Instant,
    /// Monotonic enqueue timestamp used only to report queue wait duration.
    pub(super) queued_at: Instant,
}

/// Handle for sending commands to one asynchronously running peer session.
///
/// Clones share a bounded outbound sender, actor control channel, queue counter,
/// and external state watch receiver. The handle never owns an Iroh connection;
/// `shutdown` transfers termination intent to the actor and waits for its task,
/// which cancels active and queued completions. Calls are safe from concurrent
/// Tokio tasks. Queue and shutdown errors are returned to callers rather than
/// silently losing a command.
#[derive(Clone)]
pub(super) struct PeerSession {
    tx: mpsc::Sender<QueuedDelivery>,
    control: mpsc::UnboundedSender<SessionControl>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
    queued: Arc<AtomicUsize>,
    state: watch::Receiver<ContactConnectionState>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum SessionState {
    Disconnected,
    Connecting,
    Connected,
    Closing,
}

impl SessionState {
    fn as_external(self) -> Option<ContactConnectionState> {
        match self {
            Self::Connecting => Some(ContactConnectionState::Connecting),
            Self::Connected => Some(ContactConnectionState::Connected),
            Self::Disconnected => Some(ContactConnectionState::NotConnected),
            Self::Closing => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ConnectionOrigin {
    Inbound,
    Outbound,
}

impl ConnectionOrigin {
    fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "inbound",
            Self::Outbound => "outbound",
        }
    }
}

struct ConnectionSlot {
    connection: Connection,
    origin: ConnectionOrigin,
    stop_accept: watch::Sender<bool>,
    active_handlers: usize,
    active_outbound: usize,
}

struct ActiveDelivery {
    command: QueuedDelivery,
    connection_id: Option<usize>,
    task: Option<JoinHandle<()>>,
}

enum SessionControl {
    Attach {
        connection: Connection,
        origin: ConnectionOrigin,
    },
    DialFinished {
        result: Result<Connection, String>,
    },
    ConnectionClosed {
        connection_id: usize,
        reason: String,
    },
    AcceptLoopStopped,
    InboundStream {
        connection_id: usize,
        send: iroh::endpoint::SendStream,
        recv: iroh::endpoint::RecvStream,
        permit: OwnedSemaphorePermit,
    },
    InboundFinished {
        connection_id: usize,
    },
    OutboundFinished {
        message_id: MessageId,
        connection_id: usize,
        result: Result<DeliveryReceipt, DeliveryError>,
    },
    PathChanged {
        connection_id: usize,
    },
    Shutdown(DeliveryError),
    StartOutboundDial,
}

impl PeerSession {
    /// Spawns the actor that owns a locally configured contact's peer lifecycle.
    ///
    /// `peer` is the authenticated endpoint identity, `inner` supplies shared
    /// transport resources, and `session_permit` limits the lifetime of inbound
    /// sessions. If `start_outbound` is true, the actor attempts one initial dial;
    /// otherwise it waits for an inbound connection or an explicit request. The
    /// returned handle is immediately usable and initially reports `Connecting`
    /// or `NotConnected` consistently with that choice. Spawn failure is not
    /// representable in Tokio's API; later network failures are surfaced through
    /// delivery handles and connection diagnostics.
    pub(super) fn spawn(
        peer: EndpointId,
        inner: Arc<TransportInner>,
        session_permit: Option<OwnedSemaphorePermit>,
        start_outbound: bool,
    ) -> Self {
        let (tx, rx) = mpsc::channel(OUTGOING_QUEUE_CAPACITY);
        let (control, control_rx) = mpsc::unbounded_channel();
        let queued = Arc::new(AtomicUsize::new(0));
        let queued_for_actor = queued.clone();
        let actor_control = control.clone();
        let initial_state = if start_outbound {
            ContactConnectionState::Connecting
        } else {
            ContactConnectionState::NotConnected
        };
        let (state_tx, state_rx) = watch::channel(initial_state);
        let join = tokio::spawn(async move {
            let mut actor = SessionActor::new(
                peer,
                inner,
                rx,
                control_rx,
                actor_control,
                session_permit,
                queued_for_actor,
                state_tx,
                start_outbound,
            );
            actor.run().await;
        });
        Self {
            tx,
            control,
            join: Arc::new(Mutex::new(Some(join))),
            queued,
            state: state_rx,
        }
    }

    /// Returns the latest externally visible state published by the actor.
    ///
    /// The value is a local watch snapshot, not remote presence or a guarantee
    /// that a future stream will open. It is lock-free and may race with a later
    /// actor transition, as expected for observational state.
    pub(super) fn connection_state(&self) -> ContactConnectionState {
        *self.state.borrow()
    }

    /// Attempts to reserve and enqueue one outbound delivery without awaiting.
    ///
    /// Reservation updates the shared counter before sending so concurrent
    /// callers cannot exceed the configured capacity. A full queue returns
    /// [`DeliveryError::QueueFull`], and a stopped actor returns
    /// [`DeliveryError::ShutDown`]; on either failure the reservation is released.
    /// Successful commands become actor-owned and will receive exactly one
    /// terminal completion.
    pub(super) fn try_enqueue(&self, delivery: QueuedDelivery) -> Result<(), DeliveryError> {
        let reserved = self
            .queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < OUTGOING_QUEUE_CAPACITY).then_some(current + 1)
            })
            .is_ok();
        if !reserved {
            return Err(DeliveryError::QueueFull);
        }
        match self.tx.try_send(delivery) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.queued.fetch_sub(1, Ordering::AcqRel);
                Err(DeliveryError::QueueFull)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.queued.fetch_sub(1, Ordering::AcqRel);
                Err(DeliveryError::ShutDown)
            }
        }
    }

    /// Transfers an authenticated inbound Iroh connection to the session actor.
    ///
    /// The actor applies deterministic primary/draining selection. If shutdown
    /// has already begun, it closes the connection instead. This non-blocking
    /// request intentionally has no caller-visible error because a closed actor
    /// already represents a terminal session lifecycle.
    pub(super) fn attach_inbound(&self, connection: Connection) {
        let _ = self.control.send(SessionControl::Attach {
            connection,
            origin: ConnectionOrigin::Inbound,
        });
    }

    /// Requests the actor's one allowed outbound dial when it lacks a primary.
    ///
    /// The actor ignores duplicate, concurrent, shutdown, and previously attempted
    /// requests, preserving the no-retry contract. The method is fire-and-forget;
    /// dial outcomes appear through normal state diagnostics and delivery results.
    pub(super) fn request_outbound_dial(&self) {
        let _ = self.control.send(SessionControl::StartOutboundDial);
    }

    /// Shuts down the actor and waits for all session-owned work to finish.
    ///
    /// `reason` is cloned into every active or queued delivery that must be
    /// completed during termination. The first caller consumes and awaits the
    /// join handle; concurrent or later callers only send the idempotent shutdown
    /// control message. Connection closure and completion publication are actor
    /// side effects, while this method itself reports no error.
    pub(super) async fn shutdown(&self, reason: DeliveryError) {
        let _ = self.control.send(SessionControl::Shutdown(reason));
        if let Some(join) = self.join.lock().await.take() {
            let _ = join.await;
        }
    }
}

struct SessionActor {
    peer: EndpointId,
    inner: Arc<TransportInner>,
    rx: mpsc::Receiver<QueuedDelivery>,
    control_rx: mpsc::UnboundedReceiver<SessionControl>,
    control: mpsc::UnboundedSender<SessionControl>,
    session_permit: Option<OwnedSemaphorePermit>,
    queue: VecDeque<QueuedDelivery>,
    state: SessionState,
    state_tx: watch::Sender<ContactConnectionState>,
    /// Logical session start for the current externally `Connected` period.
    connected_since: Option<StdInstant>,
    primary: Option<ConnectionSlot>,
    draining: Vec<ConnectionSlot>,
    active: Option<ActiveDelivery>,
    dial_in_progress: bool,
    dial_attempted: bool,
    start_outbound: bool,
    stopping: bool,
    queued: Arc<AtomicUsize>,
}

impl SessionActor {
    #[allow(clippy::too_many_arguments)]
    fn new(
        peer: EndpointId,
        inner: Arc<TransportInner>,
        rx: mpsc::Receiver<QueuedDelivery>,
        control_rx: mpsc::UnboundedReceiver<SessionControl>,
        control: mpsc::UnboundedSender<SessionControl>,
        session_permit: Option<OwnedSemaphorePermit>,
        queued: Arc<AtomicUsize>,
        state_tx: watch::Sender<ContactConnectionState>,
        start_outbound: bool,
    ) -> Self {
        Self {
            peer,
            inner,
            rx,
            control_rx,
            control,
            session_permit,
            queue: VecDeque::new(),
            state: SessionState::Disconnected,
            state_tx,
            connected_since: None,
            primary: None,
            draining: Vec::new(),
            active: None,
            dial_in_progress: false,
            dial_attempted: false,
            start_outbound,
            stopping: false,
            queued,
        }
    }

    async fn run(&mut self) {
        if self.start_outbound {
            self.begin_dial(Instant::now() + self.inner.config.dial_timeout)
                .await;
        }
        loop {
            if self.stopping {
                break;
            }
            self.start_next_if_possible().await;
            tokio::select! {
                biased;
                control = self.control_rx.recv() => {
                    match control {
                        Some(control) => self.handle_control(control).await,
                        None => self.stopping = true,
                    }
                }
                delivery = self.rx.recv(), if !self.stopping => {
                    match delivery {
                        Some(delivery) => self.queue.push_back(delivery),
                        None => self.stopping = true,
                    }
                }
            }
        }
        self.finish_shutdown(DeliveryError::ShutDown).await;
    }

    async fn handle_control(&mut self, control: SessionControl) {
        match control {
            SessionControl::Attach { connection, origin } => {
                if self.stopping {
                    connection.close(0u32.into(), b"session closing");
                } else {
                    self.attach_connection(connection, origin).await;
                    self.start_pending_delivery();
                }
            }
            SessionControl::DialFinished { result } => {
                self.dial_in_progress = false;
                match result {
                    Ok(connection) => {
                        self.attach_connection(connection, ConnectionOrigin::Outbound)
                            .await;
                        self.start_pending_delivery();
                    }
                    Err(error) => {
                        logging::log_warn(
                            "session",
                            if error == "connection dial timed out" {
                                "connection_dial_timed_out"
                            } else {
                                "connection_dial_finished"
                            },
                            LogFields::default()
                                .peer_str(self.peer.to_string())
                                .reason(error.clone())
                                .status("error"),
                        );
                        if self.primary.is_none() {
                            self.log_state(SessionState::Disconnected);
                        }
                        if self
                            .active
                            .as_ref()
                            .is_some_and(|active| active.connection_id.is_none())
                            && let Some(active) = self.active.take()
                        {
                            force_finish_command(
                                &active.command,
                                Err(DeliveryError::PeerNotConnected),
                            )
                            .await;
                        }
                    }
                }
            }
            SessionControl::ConnectionClosed {
                connection_id,
                reason,
            } => {
                self.connection_closed(connection_id, reason).await;
            }
            SessionControl::AcceptLoopStopped => {
                self.close_draining_finished();
            }
            SessionControl::InboundStream {
                connection_id,
                send,
                recv,
                permit,
            } => {
                if let Some(slot) = self.find_slot_mut(connection_id) {
                    slot.active_handlers += 1;
                    let peer = self.peer;
                    let inner = self.inner.clone();
                    let control = self.control.clone();
                    let connection = self
                        .find_slot_mut(connection_id)
                        .map(|slot| slot.connection.clone());
                    tokio::spawn(async move {
                        if let Some(connection) = connection {
                            handle_inbound_stream(
                                peer,
                                inner,
                                connection,
                                connection_id,
                                send,
                                recv,
                            )
                            .await;
                        } else {
                            let mut stream = IrohDeliveryStream::new(send, recv);
                            stream.reset();
                        }
                        drop(permit);
                        let _ = control.send(SessionControl::InboundFinished { connection_id });
                    });
                } else {
                    let mut stream = IrohDeliveryStream::new(send, recv);
                    stream.reset();
                    drop(permit);
                }
            }
            SessionControl::InboundFinished { connection_id } => {
                if let Some(slot) = self.find_slot_mut(connection_id) {
                    slot.active_handlers = slot.active_handlers.saturating_sub(1);
                }
                self.close_draining_finished();
            }
            SessionControl::OutboundFinished {
                message_id,
                connection_id,
                result,
            } => {
                if let Some(slot) = self.find_slot_mut(connection_id) {
                    slot.active_outbound = slot.active_outbound.saturating_sub(1);
                }
                let matches_active = self.active.as_ref().is_some_and(|active| {
                    active.command.message_id == message_id
                        && active.connection_id == Some(connection_id)
                });
                if matches_active && let Some(active) = self.active.take() {
                    finish_command(&active.command, result).await;
                }
                self.close_draining_finished();
            }
            SessionControl::PathChanged { connection_id } => {
                self.refresh_selected_path(connection_id);
            }
            SessionControl::Shutdown(reason) => {
                self.stopping = true;
                self.finish_shutdown(reason).await;
            }
            SessionControl::StartOutboundDial => {
                if self.primary.is_none() {
                    self.begin_dial(Instant::now() + self.inner.config.dial_timeout)
                        .await;
                }
            }
        }
    }

    async fn start_next_if_possible(&mut self) {
        if self.active.is_some() {
            return;
        }
        while let Some(command) = self.queue.pop_front() {
            if command.cancellation.is_cancelled() {
                self.release_queued_slot();
                continue;
            }
            if !self.inner.contacts.contains(&self.peer).await {
                self.release_queued_slot();
                finish_command(&command, Err(DeliveryError::NotAContact)).await;
                continue;
            }
            if Instant::now() >= command.deadline {
                self.release_queued_slot();
                finish_command(&command, Err(DeliveryError::TimedOut)).await;
                continue;
            }
            let Some(primary) = self.primary.as_ref() else {
                self.release_queued_slot();
                finish_command(&command, Err(DeliveryError::PeerNotConnected)).await;
                continue;
            };
            let connection = primary.connection.clone();
            self.release_queued_slot();
            self.active = Some(self.spawn_active_delivery(command, connection));
            return;
        }
    }

    fn spawn_active_delivery(
        &mut self,
        command: QueuedDelivery,
        connection: Connection,
    ) -> ActiveDelivery {
        let connection_id = connection.stable_id();
        if let Some(primary) = self.primary.as_mut()
            && primary.connection.stable_id() == connection_id
        {
            primary.active_outbound += 1;
        }
        let task = spawn_outbound_stream(
            self.peer,
            self.inner.clone(),
            connection,
            command.clone(),
            self.control.clone(),
        );
        ActiveDelivery {
            command,
            connection_id: Some(connection_id),
            task: Some(task),
        }
    }

    fn start_pending_delivery(&mut self) {
        let Some(active) = self.active.take() else {
            return;
        };
        if active.connection_id.is_some() {
            self.active = Some(active);
            return;
        }
        if active.command.cancellation.is_cancelled() || Instant::now() >= active.command.deadline {
            return;
        }
        let Some(primary) = self.primary.as_ref() else {
            self.active = Some(active);
            return;
        };
        self.active = Some(self.spawn_active_delivery(active.command, primary.connection.clone()));
    }

    async fn begin_dial(&mut self, deadline: Instant) {
        if self.dial_in_progress || self.stopping || self.dial_attempted {
            return;
        }
        self.dial_attempted = true;
        self.dial_in_progress = true;
        self.log_state(SessionState::Connecting);
        let inner = self.inner.clone();
        let peer = self.peer;
        let control = self.control.clone();
        tokio::spawn(async move {
            let result = inner.connect_for(peer, deadline).await;
            let _ = control.send(SessionControl::DialFinished { result });
        });
    }

    async fn attach_connection(&mut self, connection: Connection, origin: ConnectionOrigin) {
        let connection_id = connection.stable_id();
        let candidate_preferred = is_preferred(self.inner.endpoint.id(), self.peer, origin);
        let (stop_accept, stop_rx) = watch::channel(false);
        let slot = ConnectionSlot {
            connection: connection.clone(),
            origin,
            stop_accept,
            active_handlers: 0,
            active_outbound: 0,
        };
        self.inner.log_connection_snapshot(
            "session",
            "peer_connection_attached",
            self.peer,
            None,
            &connection,
            None,
        );
        logging::log_event(
            "session",
            "peer_connection_attached",
            LogFields::default()
                .peer_str(self.peer.to_string())
                .connection(connection_id)
                .direction(origin.as_str())
                .detail("preferred", candidate_preferred.to_string()),
        );
        spawn_connection_tasks(
            self.peer,
            self.inner.clone(),
            connection.clone(),
            origin,
            stop_rx,
            self.control.clone(),
        );

        let replace = match self.primary.as_ref() {
            None => true,
            Some(current) => {
                let current_preferred =
                    is_preferred(self.inner.endpoint.id(), self.peer, current.origin);
                candidate_preferred && !current_preferred
            }
        };
        if replace {
            if let Some(old) = self.primary.replace(slot) {
                self.start_draining(old);
            }
            logging::log_event(
                "session",
                "peer_connection_selected",
                LogFields::default()
                    .peer_str(self.peer.to_string())
                    .connection(connection_id)
                    .direction(origin.as_str()),
            );
            if self.state == SessionState::Connected {
                // Primary replacement while already connected: refresh path
                // diagnostics without resetting the logical session timestamp.
                self.refresh_selected_path(connection_id);
            } else {
                self.log_state(SessionState::Connected);
            }
        } else {
            self.start_draining(slot);
        }
    }

    fn start_draining(&mut self, slot: ConnectionSlot) {
        let _ = slot.stop_accept.send(true);
        logging::log_event(
            "session",
            "peer_connection_draining",
            LogFields::default()
                .peer_str(self.peer.to_string())
                .connection(slot.connection.stable_id())
                .direction(slot.origin.as_str())
                .detail("active_stream_handlers", slot.active_handlers.to_string())
                .detail("active_outbound_streams", slot.active_outbound.to_string()),
        );
        if slot.active_handlers == 0 && slot.active_outbound == 0 {
            slot.connection.close(0u32.into(), b"superseded connection");
        } else {
            self.draining.push(slot);
        }
    }

    async fn connection_closed(&mut self, connection_id: usize, reason: String) {
        let lost_connection = self
            .primary
            .as_ref()
            .filter(|slot| slot.connection.stable_id() == connection_id)
            .map(|slot| slot.connection.clone())
            .or_else(|| {
                self.draining
                    .iter()
                    .find(|slot| slot.connection.stable_id() == connection_id)
                    .map(|slot| slot.connection.clone())
            });
        let was_primary = self
            .primary
            .as_ref()
            .is_some_and(|slot| slot.connection.stable_id() == connection_id);
        let mut removed = false;
        if was_primary {
            self.primary = None;
            removed = true;
            self.log_state(SessionState::Disconnected);
        } else if let Some(index) = self
            .draining
            .iter()
            .position(|slot| slot.connection.stable_id() == connection_id)
        {
            self.draining.swap_remove(index);
            removed = true;
        }
        if !removed {
            return;
        }
        if let Some(connection) = lost_connection.as_ref() {
            self.inner.log_connection_snapshot(
                "session",
                "peer_connection_lost_snapshot",
                self.peer,
                None,
                connection,
                None,
            );
        }
        logging::log_event(
            "session",
            "peer_connection_lost",
            LogFields::default()
                .peer_str(self.peer.to_string())
                .connection(connection_id)
                .reason(reason),
        );
        if self
            .active
            .as_ref()
            .is_some_and(|active| active.connection_id == Some(connection_id))
            && let Some(active) = self.active.take()
        {
            active.command.cancellation.cancel();
            if let Some(task) = active.task {
                task.abort();
            }
            force_finish_command(
                &active.command,
                Err(DeliveryError::Transport("connection lost".into())),
            )
            .await;
        }
    }

    fn find_slot_mut(&mut self, connection_id: usize) -> Option<&mut ConnectionSlot> {
        if self
            .primary
            .as_ref()
            .is_some_and(|slot| slot.connection.stable_id() == connection_id)
        {
            return self.primary.as_mut();
        }
        self.draining
            .iter_mut()
            .find(|slot| slot.connection.stable_id() == connection_id)
    }

    fn close_draining_finished(&mut self) {
        let mut keep = Vec::with_capacity(self.draining.len());
        for slot in self.draining.drain(..) {
            if slot.active_handlers == 0 && slot.active_outbound == 0 {
                slot.connection.close(0u32.into(), b"draining complete");
            } else {
                keep.push(slot);
            }
        }
        self.draining = keep;
    }

    async fn finish_shutdown(&mut self, reason: DeliveryError) {
        if self.state != SessionState::Closing {
            self.log_state(SessionState::Closing);
        }
        self.stopping = true;
        if let Some(active) = self.active.take() {
            if let Some(task) = active.task {
                task.abort();
            }
            force_finish_command(&active.command, Err(reason.clone())).await;
        }
        while let Some(command) = self.queue.pop_front() {
            self.release_queued_slot();
            finish_command(&command, Err(reason.clone())).await;
        }
        while let Ok(command) = self.rx.try_recv() {
            self.release_queued_slot();
            finish_command(&command, Err(reason.clone())).await;
        }
        if let Some(primary) = self.primary.take() {
            primary.connection.close(0u32.into(), b"session shutdown");
        }
        for draining in self.draining.drain(..) {
            draining.connection.close(0u32.into(), b"session shutdown");
        }
        self.dial_in_progress = false;
        self.session_permit.take();
    }

    fn release_queued_slot(&self) {
        self.queued.fetch_sub(1, Ordering::AcqRel);
    }

    fn log_state(&mut self, state: SessionState) {
        if self.state == state {
            return;
        }
        self.state = state;
        logging::log_event(
            "session",
            "peer_session_state_changed",
            LogFields::default()
                .peer_str(self.peer.to_string())
                .status(format!("{state:?}")),
        );
        if let Some(external) = state.as_external() {
            self.publish_external_state(external);
        }
    }

    /// Publishes an enriched connection update for the current external state.
    ///
    /// Records `connected_since` on the first transition to `Connected`, reuses it
    /// for later Connected publishes (including path refreshes), and clears it on
    /// `NotConnected` / `Connecting`.
    fn publish_external_state(&mut self, state: ContactConnectionState) {
        let (selected_path, connected_since) = match state {
            ContactConnectionState::Connected => {
                if self.connected_since.is_none() {
                    self.connected_since = Some(StdInstant::now());
                }
                let selected_path = self
                    .primary
                    .as_ref()
                    .map(|slot| selected_path_from_connection(&slot.connection))
                    .unwrap_or_else(SelectedPath::unknown);
                (selected_path, self.connected_since)
            }
            ContactConnectionState::Connecting | ContactConnectionState::NotConnected => {
                self.connected_since = None;
                (SelectedPath::unknown(), None)
            }
        };
        let _ = self.state_tx.send(state);
        self.inner.emit_connection_event(PeerConnectionEvent {
            peer_id: PeerId::from_canonical(self.peer.to_string()),
            state,
            selected_path,
            connected_since,
        });
    }

    /// Re-reads the selected-path snapshot for the primary connection and emits a
    /// Connected update when the event still belongs to the current primary.
    ///
    /// Updates from draining or replaced connections are ignored. The logical
    /// session timestamp is preserved.
    fn refresh_selected_path(&mut self, connection_id: usize) {
        let primary_id = self
            .primary
            .as_ref()
            .map(|slot| slot.connection.stable_id());
        if !should_apply_path_refresh(primary_id, connection_id, self.state) {
            return;
        }
        let Some(primary) = self.primary.as_ref() else {
            return;
        };
        let selected_path = selected_path_from_connection(&primary.connection);
        if self.connected_since.is_none() {
            self.connected_since = Some(StdInstant::now());
        }
        let connected_since = self.connected_since;
        self.inner.log_connection_snapshot(
            "session",
            "peer_connection_path_refreshed",
            self.peer,
            None,
            &primary.connection,
            None,
        );
        self.inner.emit_connection_event(PeerConnectionEvent {
            peer_id: PeerId::from_canonical(self.peer.to_string()),
            state: ContactConnectionState::Connected,
            selected_path,
            connected_since,
        });
    }
}

fn is_preferred(local: EndpointId, remote: EndpointId, origin: ConnectionOrigin) -> bool {
    match origin {
        ConnectionOrigin::Outbound => local < remote,
        ConnectionOrigin::Inbound => local > remote,
    }
}

/// Returns whether a path-refresh event should update diagnostics for the session.
///
/// Path updates are accepted only from the current primary connection while the
/// external session state is `Connected`. Draining or replaced connection IDs are
/// ignored, including after the primary has already been cleared.
fn should_apply_path_refresh(
    primary_connection_id: Option<usize>,
    event_connection_id: usize,
    state: SessionState,
) -> bool {
    primary_connection_id == Some(event_connection_id) && state == SessionState::Connected
}

fn spawn_connection_tasks(
    peer: EndpointId,
    inner: Arc<TransportInner>,
    connection: Connection,
    origin: ConnectionOrigin,
    mut stop_rx: watch::Receiver<bool>,
    control: mpsc::UnboundedSender<SessionControl>,
) {
    let connection_id = connection.stable_id();
    let accept_control = control.clone();
    let accept_inner = inner.clone();
    let accept_connection = connection.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
                accepted = accept_connection.accept_bi() => {
                    match accepted {
                        Ok((send, recv)) => {
                            let Some(permit) = accept_inner.inbound_stream_handlers.clone().try_acquire_owned().ok() else {
                                logging::log_warn(
                                    "session",
                                    "inbound_stream_limit_reached",
                                    LogFields::default()
                                        .peer_str(peer.to_string())
                                        .connection(connection_id)
                                        .reason("stream_handler_budget"),
                                );
                                let stream_id = u64::from(send.id());
                                let mut stream = IrohDeliveryStream::new(send, recv);
                                stream.reset();
                                logging::log_warn(
                                    "session",
                                    "stream_reset",
                                    LogFields::default()
                                        .peer_str(peer.to_string())
                                        .connection(connection_id)
                                        .stream(stream_id)
                                        .reason("stream_handler_budget"),
                                );
                                continue;
                            };
                            logging::log_event(
                                "session",
                                "inbound_stream_accepted",
                                LogFields::default()
                                    .peer_str(peer.to_string())
                                    .connection(connection_id)
                                    .stream(u64::from(send.id()))
                                    .direction(origin.as_str()),
                            );
                            if accept_control.send(SessionControl::InboundStream { connection_id, send, recv, permit }).is_err() {
                                break;
                            }
                        }
                        Err(error) => {
                            let _ = accept_control.send(SessionControl::AcceptLoopStopped);
                            logging::log_warn(
                                "session",
                                "accept_loop_stopped",
                                LogFields::default()
                                    .peer_str(peer.to_string())
                                    .connection(connection_id)
                                    .error(&error),
                            );
                            break;
                        }
                    }
                }
            }
        }
        let _ = accept_control.send(SessionControl::AcceptLoopStopped);
    });

    let monitor_control = control.clone();
    let monitor_connection = connection.clone();
    tokio::spawn(async move {
        let reason = monitor_connection.closed().await;
        let _ = monitor_control.send(SessionControl::ConnectionClosed {
            connection_id,
            reason: format!("{reason:?}"),
        });
    });

    let path_control = control;
    let path_peer = peer;
    tokio::spawn(async move {
        let mut events = connection.path_events();
        while let Some(event) = events.next().await {
            // Path events are observation-only: log the transition, then ask
            // the session actor to re-read the selected-path snapshot. This
            // must not await network work, start a second dial, or touch an
            // active delivery.
            let details = path_event_details(&event);
            let mut fields = LogFields::default()
                .peer_str(path_peer.to_string())
                .connection(connection_id)
                .detail("path_event_kind", details.kind);
            if let Some(path_id) = details.path_id {
                fields = fields.detail("path_id", path_id);
            }
            if let Some(path_kind) = details.path_kind {
                fields = fields.detail("path_kind", path_kind);
            }
            if let Some(path_remote) = details.path_remote {
                fields = fields.detail("path_remote", path_remote);
            }
            if let Some(missed) = details.missed {
                fields = fields.detail("path_events_missed", missed.to_string());
            }
            logging::log_event("session", "peer_connection_path_event", fields);
            if path_control
                .send(SessionControl::PathChanged { connection_id })
                .is_err()
            {
                break;
            }
        }
    });
}

/// Spawns one outbound exchange while retaining completion ownership in `SessionActor`.
///
/// The task performs network I/O only; it forwards the result to the actor through
/// [`SessionControl::OutboundFinished`] after resolving the command completion. This
/// preserves the actor's ownership of queue capacity, active-delivery tracking, and
/// connection lifecycle regardless of whether one stream succeeds or fails.
fn spawn_outbound_stream(
    peer: EndpointId,
    inner: Arc<TransportInner>,
    connection: Connection,
    command: QueuedDelivery,
    control: mpsc::UnboundedSender<SessionControl>,
) -> JoinHandle<()> {
    let connection_id = connection.stable_id();
    tokio::spawn(async move {
        let message_id = command.message_id;
        let result = run_outbound_stream(peer, inner, connection, command.clone()).await;
        finish_command(&command, result.clone()).await;
        let _ = control.send(SessionControl::OutboundFinished {
            message_id,
            connection_id,
            result,
        });
    })
}

/// Opens and exchanges one outbound delivery stream on an actor-selected connection.
///
/// The caller has already reserved outbound capacity and selected `connection`.
/// Opening the stream observes the queued command's absolute deadline and
/// cancellation token. Once opened, the Iroh stream halves are adapted to the
/// transport-neutral delivery policy, which owns framing, receipt validation, and
/// best-effort reset on exchange errors. This function only returns that result: it
/// never resolves a handle or changes session or connection ownership.
async fn run_outbound_stream(
    peer: EndpointId,
    inner: Arc<TransportInner>,
    connection: Connection,
    command: QueuedDelivery,
) -> Result<DeliveryReceipt, DeliveryError> {
    let connection_id = connection.stable_id();
    logging::log_event(
        "session",
        "message_delivery_started",
        LogFields::default()
            .peer_str(peer.to_string())
            .message(&command.message_id)
            .connection(connection.stable_id())
            .detail(
                "queue_wait_ms",
                Instant::now()
                    .saturating_duration_since(command.queued_at)
                    .as_millis()
                    .to_string(),
            ),
    );
    let (send, recv) = match with_deadline(
        command.deadline,
        connection.open_bi(),
        command.cancellation.as_ref(),
    )
    .await
    {
        Ok(Ok(streams)) => streams,
        Ok(Err(error)) => return Err(DeliveryError::Transport(error.to_string())),
        Err(error) => return Err(error),
    };
    let stream_id = u64::from(send.id());
    let message_id = command.message_id;
    let deadline = command.deadline;
    let cancellation = Arc::clone(&command.cancellation);
    let envelope = command.envelope;
    let mut stream = IrohDeliveryStream::new(send, recv);
    let result = outbound_exchange(
        &mut stream,
        envelope,
        deadline,
        cancellation.as_ref(),
        |stage| {
            log_delivery_stage(
                &inner,
                peer,
                &connection,
                Some(&message_id),
                Some(stream_id),
                stage,
            );
        },
    )
    .await;
    if let Err(error) = &result {
        log_stream_reset(peer, connection_id, Some(&message_id), stream_id, error);
    }
    result
}

/// Emits delivery-exchange diagnostics without transferring session ownership.
///
/// `stream_id` is captured before adapting the Iroh halves, so every emitted
/// snapshot preserves the peer, message, connection, stream, and selected-path
/// fields used by existing session observability. `message_id` remains optional
/// because malformed inbound bytes never yield a validated identifier and must not
/// be represented by a fabricated value.
fn log_delivery_stage(
    inner: &TransportInner,
    peer: EndpointId,
    connection: &Connection,
    message_id: Option<&MessageId>,
    stream_id: Option<u64>,
    stage: DeliveryStage,
) {
    let event = match stage {
        DeliveryStage::StreamOpened => "stream_opened",
        DeliveryStage::TextFrameWritten => "text_frame_written",
        DeliveryStage::TextFrameReceived { .. } => "text_frame_received",
        DeliveryStage::ReceiptReceived => "receipt_received",
        DeliveryStage::ReceiptWritten => "receipt_write_finished",
    };
    inner.log_connection_snapshot("session", event, peer, message_id, connection, stream_id);
}

/// Runs one inbound delivery exchange on an already-accepted Iroh stream.
///
/// The connection task has called `accept_bi` exactly once before transferring
/// ownership of its halves here. This function adapts those halves for
/// [`inbound_exchange`], supplies the current allowlist callback immediately
/// before publication, and records exchange-level diagnostics. Delivery policy
/// resets failures itself, so this function only logs its error and never resets
/// the Iroh stream a second time. A successful rejection is normal completion and
/// leaves the connection available for future streams.
async fn handle_inbound_stream(
    peer: EndpointId,
    inner: Arc<TransportInner>,
    connection: Connection,
    connection_id: usize,
    send: iroh::endpoint::SendStream,
    recv: iroh::endpoint::RecvStream,
) {
    let mut stream = IrohDeliveryStream::new(send, recv);
    let stream_id = stream.stream_id();
    let cancellation = DeliveryCancellation::new();
    let authorisation_inner = inner.clone();
    let mut message_id = None;
    let result = inbound_exchange(
        &mut stream,
        PeerId::from_canonical(peer.to_string()),
        &inner.incoming_tx,
        Instant::now() + INBOUND_STREAM_TIMEOUT,
        &cancellation,
        move || async move { authorisation_inner.contacts.contains(&peer).await },
        |stage| {
            if let DeliveryStage::TextFrameReceived {
                message_id: decoded_message_id,
            } = stage
            {
                message_id = Some(decoded_message_id);
            }
            log_delivery_stage(
                &inner,
                peer,
                &connection,
                message_id.as_ref(),
                Some(stream_id),
                stage,
            );
        },
    )
    .await;
    if let Err(error) = result {
        log_stream_reset(peer, connection_id, message_id.as_ref(), stream_id, error);
    }
}

async fn finish_command(command: &QueuedDelivery, result: Result<DeliveryReceipt, DeliveryError>) {
    if command.cancellation.cancel() {
        resolve_once(&command.completion, result).await;
    }
}

async fn force_finish_command(
    command: &QueuedDelivery,
    result: Result<DeliveryReceipt, DeliveryError>,
) {
    command.cancellation.cancel();
    resolve_once(&command.completion, result).await;
}

/// Records a failed delivery exchange after delivery policy reset its stream.
///
/// The delivery layer has already issued its idempotent reset before returning the
/// error. This helper deliberately performs no Iroh operation, preserving the
/// warning event without risking a second reset or changing connection ownership.
/// `message_id` is absent when the failing inbound bytes could not be decoded into
/// a valid text frame.
fn log_stream_reset(
    peer: EndpointId,
    connection_id: usize,
    message_id: Option<&MessageId>,
    stream_id: u64,
    reason: impl std::fmt::Display,
) {
    let mut fields = LogFields::default()
        .peer_str(peer.to_string())
        .connection(connection_id)
        .stream(stream_id)
        .reason(reason.to_string());
    if let Some(message_id) = message_id {
        fields = fields.message(message_id);
    }
    logging::log_warn("session", "stream_reset", fields);
}

#[cfg(test)]
mod tests {
    use iroh::SecretKey;

    use super::*;

    #[test]
    fn canonical_connection_origin_is_deterministic() {
        let first = SecretKey::from_bytes(&[1; 32]).public();
        let second = SecretKey::from_bytes(&[2; 32]).public();
        let (lower, higher) = if first < second {
            (first, second)
        } else {
            (second, first)
        };

        assert!(is_preferred(lower, higher, ConnectionOrigin::Outbound));
        assert!(is_preferred(higher, lower, ConnectionOrigin::Inbound));
        assert!(!is_preferred(higher, lower, ConnectionOrigin::Outbound));
        assert!(!is_preferred(lower, higher, ConnectionOrigin::Inbound));
    }

    #[test]
    fn path_refresh_accepts_only_primary_connected_connection() {
        assert!(should_apply_path_refresh(
            Some(7),
            7,
            SessionState::Connected
        ));
        assert!(!should_apply_path_refresh(
            Some(7),
            8,
            SessionState::Connected
        ));
        assert!(!should_apply_path_refresh(None, 7, SessionState::Connected));
        assert!(!should_apply_path_refresh(
            Some(7),
            7,
            SessionState::Connecting
        ));
        assert!(!should_apply_path_refresh(
            Some(7),
            7,
            SessionState::Disconnected
        ));
    }

    #[test]
    fn logical_connected_since_is_cleared_outside_connected() {
        let mut connected_since = Some(StdInstant::now());
        match ContactConnectionState::NotConnected {
            ContactConnectionState::Connected => {}
            ContactConnectionState::Connecting | ContactConnectionState::NotConnected => {
                connected_since = None;
            }
        }
        assert!(connected_since.is_none());

        let first = StdInstant::now();
        let mut connected_since = Some(first);
        // Path-only Connected update reuses the existing timestamp.
        if connected_since.is_none() {
            connected_since = Some(StdInstant::now());
        }
        assert_eq!(connected_since, Some(first));
    }
}
