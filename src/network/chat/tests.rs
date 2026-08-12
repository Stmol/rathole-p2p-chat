use std::time::Duration;

use iroh::{Endpoint, EndpointAddr, SecretKey, TransportAddr, endpoint::presets};
use tokio::sync::{mpsc, oneshot};

use super::framing::{read_document, write_document};
use super::transport::{
    exhaust_inbound_handler_budget, exhaust_inbound_session_budget, inbound_handler_budget,
    inbound_session_budget, set_test_route,
};
use super::{
    CHAT_ALPN, ChatClient, ChatTransport, ChatTransportConfig, DeliveryError,
    INBOUND_STREAM_TIMEOUT, IncomingText, MAX_OUTBOUND_DIALS, OUTGOING_QUEUE_CAPACITY,
    PeerConnectionEvent,
};
use crate::domain::connection::ContactConnectionState;
use crate::domain::identity::PeerId;
use crate::network::identity::peer_id_from_secret;
use crate::protocol::{
    ChatFrame, MAX_FRAME_BYTES, MAX_TEXT_BYTES, MessageId, RejectionCode, ValidationError,
    WireEnvelope,
};
use iroh::EndpointId;

struct TestPeer {
    peer_id: PeerId,
    endpoint_id: EndpointId,
    transport: ChatTransport,
    client: ChatClient,
    incoming: mpsc::Receiver<IncomingText>,
    connection_events: mpsc::UnboundedReceiver<PeerConnectionEvent>,
}

fn handshake_transport_config() -> ChatTransportConfig {
    ChatTransportConfig {
        dial_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

async fn local_peer(secret: SecretKey, contacts: impl IntoIterator<Item = PeerId>) -> TestPeer {
    local_peer_with_config(secret, contacts, handshake_transport_config()).await
}

async fn local_peer_with_config(
    secret: SecretKey,
    contacts: impl IntoIterator<Item = PeerId>,
    config: ChatTransportConfig,
) -> TestPeer {
    let peer_id = peer_id_from_secret(&secret);
    let endpoint_id = secret.public();
    let (transport, client, incoming, connection_events) =
        ChatTransport::start_with_config(secret, contacts, config)
            .await
            .unwrap();
    TestPeer {
        peer_id,
        endpoint_id,
        transport,
        client,
        incoming,
        connection_events,
    }
}

async fn wait_for_peer_state(peer: &TestPeer, remote: &PeerId, expected: ContactConnectionState) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if peer.client.connection_state(remote).await == Some(expected) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected:?} for {remote:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_connection_event(
    events: &mut mpsc::UnboundedReceiver<PeerConnectionEvent>,
    peer_id: &PeerId,
    expected: ContactConnectionState,
) -> PeerConnectionEvent {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let event = tokio::time::timeout_at(deadline, events.recv())
            .await
            .expect("timed out waiting for connection event")
            .expect("connection event channel closed");
        if &event.peer_id == peer_id && event.state == expected {
            return event;
        }
    }
}

async fn direct_loopback_addr(transport: &ChatTransport) -> EndpointAddr {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut addr = transport.endpoint().addr();
        addr.addrs.retain(|transport_addr| {
            matches!(
                transport_addr,
                TransportAddr::Ip(socket) if socket.ip().is_loopback()
            )
        });
        if addr.ip_addrs().next().is_some() {
            return addr;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for local direct addresses"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn direct_loopback_endpoint_addr(endpoint: &Endpoint) -> EndpointAddr {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let mut addr = endpoint.addr();
        addr.addrs.retain(|transport_addr| {
            matches!(
                transport_addr,
                TransportAddr::Ip(socket) if socket.ip().is_loopback()
            )
        });
        if addr.ip_addrs().next().is_some() {
            return addr;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for local direct addresses"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Verifies an invalid inbound stream receives no application receipt.
///
/// Delivery policy resets protocol failures, so Iroh may report either a reset
/// transport error or an early end of stream. Both are failures to receive an
/// `Accepted` or `Rejected` document and are therefore the required outcome.
async fn assert_invalid_stream_has_no_receipt(recv: &mut iroh::endpoint::RecvStream) {
    let result = tokio::time::timeout(Duration::from_secs(2), read_document(recv)).await;
    assert!(
        matches!(result, Ok(Err(_))),
        "invalid inbound stream unexpectedly received a receipt: {result:?}"
    );
}

async fn connect_test_routes(alice: &TestPeer, bob: &TestPeer) {
    let alice_addr = direct_loopback_addr(&alice.transport).await;
    let bob_addr = direct_loopback_addr(&bob.transport).await;
    set_test_route(alice.transport.inner(), bob.endpoint_id, bob_addr).await;
    set_test_route(bob.transport.inner(), alice.endpoint_id, alice_addr).await;
}

async fn local_pair() -> (TestPeer, TestPeer) {
    let alice_secret = SecretKey::from_bytes(&[51; 32]);
    let bob_secret = SecretKey::from_bytes(&[52; 32]);
    let alice_id = peer_id_from_secret(&alice_secret);
    let bob_id = peer_id_from_secret(&bob_secret);

    let alice = local_peer(alice_secret, []).await;
    let bob = local_peer(bob_secret, []).await;
    connect_test_routes(&alice, &bob).await;
    alice
        .transport
        .replace_contacts([bob_id.clone()])
        .await
        .unwrap();
    bob.transport
        .replace_contacts([alice_id.clone()])
        .await
        .unwrap();
    wait_for_peer_state(&alice, &bob_id, ContactConnectionState::Connected).await;
    wait_for_peer_state(&bob, &alice_id, ContactConnectionState::Connected).await;
    (alice, bob)
}

#[tokio::test]
async fn connected_session_emits_selected_path_diagnostics_and_preserves_duration() {
    let (mut alice, bob) = local_pair().await;

    let connected = wait_for_connection_event(
        &mut alice.connection_events,
        &bob.peer_id,
        ContactConnectionState::Connected,
    )
    .await;
    assert!(connected.connected_since.is_some());
    // Direct loopback routes typically select an IP path; accept relay/custom too
    // as long as the diagnostic is populated from a live snapshot.
    assert!(
        matches!(
            connected.selected_path.kind,
            crate::domain::connection::SelectedPathKind::DirectIp
                | crate::domain::connection::SelectedPathKind::Relay
                | crate::domain::connection::SelectedPathKind::Custom
                | crate::domain::connection::SelectedPathKind::Unknown
        ),
        "unexpected path kind: {:?}",
        connected.selected_path.kind
    );
    if connected.selected_path.kind != crate::domain::connection::SelectedPathKind::Unknown {
        let address = connected
            .selected_path
            .remote_address
            .as_deref()
            .expect("selected path should expose a remote address");
        assert!(
            address.starts_with("ip:")
                || address.starts_with("relay:")
                || address.starts_with("custom:"),
            "unexpected address format: {address}"
        );
    }

    let first_since = connected.connected_since;

    // Drain any immediate path-refresh events and ensure the logical timestamp is reused.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while let Ok(Some(event)) =
        tokio::time::timeout_at(deadline, alice.connection_events.recv()).await
    {
        if event.state == ContactConnectionState::Connected {
            assert_eq!(event.connected_since, first_since);
        }
    }

    // Query API remains state-only.
    assert_eq!(
        alice.client.connection_state(&bob.peer_id).await,
        Some(ContactConnectionState::Connected)
    );

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn connected_session_admits_first_message_without_path_kind_gate() {
    let (alice, mut bob) = local_pair().await;

    // Connected is sufficient for admission; send_text does not wait for DirectIp.
    assert_eq!(
        alice.client.connection_state(&bob.peer_id).await,
        Some(ContactConnectionState::Connected)
    );
    let handle = alice
        .client
        .send_text(bob.peer_id.clone(), "first without path gate")
        .await
        .expect("connected session should admit send without waiting for DirectIp");
    let incoming = tokio::time::timeout(Duration::from_secs(5), bob.incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.message_id, handle.message_id);
    assert_eq!(handle.wait().await.unwrap().message_id, incoming.message_id);

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn active_delivery_keeps_single_outcome_through_path_refreshes() {
    let (mut alice, mut bob) = local_pair().await;
    let first_since = wait_for_connection_event(
        &mut alice.connection_events,
        &bob.peer_id,
        ContactConnectionState::Connected,
    )
    .await
    .connected_since;

    let recv = tokio::spawn(async move { bob.incoming.recv().await });
    let handle = alice
        .client
        .send_text(bob.peer_id.clone(), "in flight during path refresh")
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(event)) =
            tokio::time::timeout(Duration::from_millis(50), alice.connection_events.recv()).await
        {
            assert_ne!(event.state, ContactConnectionState::NotConnected);
            if event.state == ContactConnectionState::Connected {
                assert_eq!(event.connected_since, first_since);
            }
        }
    }

    let incoming = recv.await.unwrap().unwrap();
    let receipt = handle.wait().await.unwrap();
    assert_eq!(receipt.message_id, incoming.message_id);
    assert_eq!(
        alice.client.connection_state(&bob.peer_id).await,
        Some(ContactConnectionState::Connected)
    );

    alice.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn path_refreshes_preserve_connected_since_across_kind_changes() {
    let (mut alice, bob) = local_pair().await;
    let first_since = wait_for_connection_event(
        &mut alice.connection_events,
        &bob.peer_id,
        ContactConnectionState::Connected,
    )
    .await
    .connected_since;

    let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
    while let Ok(Some(event)) =
        tokio::time::timeout_at(deadline, alice.connection_events.recv()).await
    {
        if event.state == ContactConnectionState::Connected {
            assert_eq!(event.connected_since, first_since);
        }
    }

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn authorised_contacts_exchange_unicode_text_and_an_ack() {
    let (alice, mut bob) = local_pair().await;

    let handle = alice
        .client
        .send_text(bob.peer_id.clone(), "Привет, 👋\nRathole")
        .await
        .unwrap();
    let incoming = tokio::time::timeout(Duration::from_secs(5), bob.incoming.recv())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(incoming.peer_id, alice.peer_id);
    assert_eq!(incoming.body, "Привет, 👋\nRathole");
    assert_eq!(incoming.message_id, handle.message_id);
    assert_eq!(handle.wait().await.unwrap().message_id, incoming.message_id);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), bob.incoming.recv())
            .await
            .is_err(),
        "one inbound Text must publish exactly one IncomingText"
    );

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn subsequent_delivery_after_idle_keeps_the_connection_alive() {
    let (alice, mut bob) = local_pair().await;
    let first = alice
        .client
        .send_text(bob.peer_id.clone(), "before idle")
        .await
        .unwrap();
    let first_message_id = first.message_id;
    assert_eq!(bob.incoming.recv().await.unwrap().body, "before idle");
    let first_receipt = first.wait().await.unwrap();
    assert_eq!(first_receipt.message_id, first_message_id);
    assert_eq!(
        alice.client.connection_state(&bob.peer_id).await,
        Some(ContactConnectionState::Connected),
        "a matching Accepted receipt must leave the primary connection reusable"
    );

    tokio::time::sleep(INBOUND_STREAM_TIMEOUT + Duration::from_millis(250)).await;

    let second = alice
        .client
        .send_text(bob.peer_id.clone(), "after idle")
        .await
        .unwrap();
    let second_message_id = second.message_id;
    assert_eq!(bob.incoming.recv().await.unwrap().body, "after idle");
    assert_eq!(second.wait().await.unwrap().message_id, second_message_id);

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_survives_slow_remote_read() {
    let sender_secret = SecretKey::from_bytes(&[103; 32]);
    let receiver_secret = SecretKey::from_bytes(&[104; 32]);
    let sender_id = peer_id_from_secret(&sender_secret);
    let mut receiver = local_peer(receiver_secret, [sender_id.clone()]).await;
    let sender = Endpoint::builder(presets::N0)
        .secret_key(sender_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    let connection = sender
        .connect(direct_loopback_addr(&receiver.transport).await, CHAT_ALPN)
        .await
        .unwrap();
    let message_id = MessageId::new([105; 16]);
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    write_document(
        &mut send,
        &WireEnvelope::new(ChatFrame::text(message_id, 1, "slow receipt").unwrap()),
    )
    .await
    .unwrap();
    send.finish().unwrap();

    let incoming = tokio::time::timeout(Duration::from_secs(2), receiver.incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.message_id, message_id);
    tokio::time::sleep(INBOUND_STREAM_TIMEOUT + Duration::from_secs(1)).await;
    let receipt = tokio::time::timeout(Duration::from_secs(2), read_document(&mut recv))
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(receipt.frame, ChatFrame::Accepted { message_id: id, .. } if id == message_id)
    );

    sender.close().await;
    receiver.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_peer_session_preserves_queue_order_across_two_message_streams() {
    let (alice, mut bob) = local_pair().await;
    let first = alice
        .client
        .send_text(bob.peer_id.clone(), "first")
        .await
        .unwrap();
    let second = alice
        .client
        .send_text(bob.peer_id.clone(), "second")
        .await
        .unwrap();

    assert_eq!(bob.incoming.recv().await.unwrap().body, "first");
    assert_eq!(bob.incoming.recv().await.unwrap().body, "second");
    first.wait().await.unwrap();
    second.wait().await.unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn unauthorised_sender_gets_unknown_contact_without_delivery() {
    let (mut alice, bob) = local_pair().await;
    // Update only the live allowlist: `replace_contacts` intentionally tears
    // down removed sessions, while this test must exercise the exchange-time
    // authorisation callback on an otherwise healthy accepted connection.
    alice
        .transport
        .inner()
        .contacts
        .replace_peer_ids([])
        .await
        .unwrap();

    let result = bob
        .client
        .send_text(alice.peer_id.clone(), "not authorised by Alice")
        .await
        .unwrap()
        .wait()
        .await;
    assert!(matches!(
        result,
        Err(DeliveryError::Rejected(RejectionCode::UnknownContact))
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), alice.incoming.recv())
            .await
            .is_err()
    );
    assert_eq!(
        bob.client.connection_state(&alice.peer_id).await,
        Some(ContactConnectionState::Connected),
        "a rejected stream must not close its healthy peer connection"
    );

    alice
        .transport
        .inner()
        .contacts
        .replace_peer_ids([bob.peer_id.clone()])
        .await
        .unwrap();
    let accepted = bob
        .client
        .send_text(alice.peer_id.clone(), "connection remains usable")
        .await
        .unwrap();
    let incoming = tokio::time::timeout(Duration::from_secs(2), alice.incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.message_id, accepted.message_id);
    assert_eq!(
        accepted.wait().await.unwrap().message_id,
        incoming.message_id
    );

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_body_fails_before_transport_work() {
    let (alice, bob) = local_pair().await;
    assert!(matches!(
        alice
            .client
            .send_text(bob.peer_id, "x".repeat(MAX_TEXT_BYTES + 1))
            .await,
        Err(DeliveryError::Validation(ValidationError::TextTooLarge))
    ));
    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn simultaneous_bidirectional_sends_are_delivered_without_global_order_claim() {
    let (mut alice, mut bob) = local_pair().await;
    let alice_handle = alice
        .client
        .send_text(bob.peer_id.clone(), "from Alice")
        .await
        .unwrap();
    let bob_handle = bob
        .client
        .send_text(alice.peer_id.clone(), "from Bob")
        .await
        .unwrap();

    assert_eq!(alice.incoming.recv().await.unwrap().body, "from Bob");
    assert_eq!(bob.incoming.recv().await.unwrap().body, "from Alice");
    alice_handle.wait().await.unwrap();
    bob_handle.wait().await.unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn reply_can_start_before_the_first_delivery_handle_settles() {
    let (mut alice, mut bob) = local_pair().await;
    let first = alice
        .client
        .send_text(bob.peer_id.clone(), "from Alice")
        .await
        .unwrap();

    assert_eq!(bob.incoming.recv().await.unwrap().body, "from Alice");
    let reply = bob
        .client
        .send_text(alice.peer_id.clone(), "from Bob")
        .await
        .unwrap();
    assert_eq!(alice.incoming.recv().await.unwrap().body, "from Bob");

    first.wait().await.unwrap();
    reply.wait().await.unwrap();
    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn sequential_reply_works_after_alice_establishes_connection() {
    let (mut alice, mut bob) = local_pair().await;

    let first = alice
        .client
        .send_text(bob.peer_id.clone(), "from Alice")
        .await
        .unwrap();

    assert_eq!(bob.incoming.recv().await.unwrap().body, "from Alice");
    first.wait().await.unwrap();

    let reply = bob
        .client
        .send_text(alice.peer_id.clone(), "from Bob")
        .await
        .unwrap();

    let incoming = tokio::time::timeout(Duration::from_secs(2), alice.incoming.recv())
        .await
        .expect("reply should reach Alice")
        .expect("Alice incoming channel should remain open");

    assert_eq!(incoming.body, "from Bob");
    reply.wait().await.unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn sequential_reply_works_after_bob_establishes_connection() {
    let (mut alice, mut bob) = local_pair().await;

    let first = bob
        .client
        .send_text(alice.peer_id.clone(), "from Bob")
        .await
        .unwrap();

    assert_eq!(alice.incoming.recv().await.unwrap().body, "from Bob");
    first.wait().await.unwrap();

    let reply = alice
        .client
        .send_text(bob.peer_id.clone(), "from Alice")
        .await
        .unwrap();

    let incoming = tokio::time::timeout(Duration::from_secs(2), bob.incoming.recv())
        .await
        .expect("reply should reach Bob")
        .expect("Bob incoming channel should remain open");

    assert_eq!(incoming.body, "from Alice");
    reply.wait().await.unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn incoming_backpressure_holds_the_sixty_fifth_accepted() {
    let (alice, mut bob) = local_pair().await;

    let mut completed = Vec::new();
    for index in 0..64 {
        let handle = alice
            .client
            .send_text(bob.peer_id.clone(), format!("queued-{index}"))
            .await
            .unwrap();
        completed.push(handle);
    }
    for handle in completed {
        tokio::time::timeout(Duration::from_secs(10), handle.wait())
            .await
            .expect("delivery within timeout")
            .unwrap();
    }

    let blocked = alice
        .client
        .send_text(bob.peer_id.clone(), "blocked-65")
        .await
        .unwrap();
    let wait_task = tokio::spawn(async move { blocked.wait().await });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!wait_task.is_finished());

    let _ = bob.incoming.recv().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), wait_task)
        .await
        .expect("65th delivery unblocked")
        .unwrap()
        .unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn removing_contact_after_delivery_blocks_the_next_send() {
    let (alice, mut bob) = local_pair().await;
    let handle = alice
        .client
        .send_text(bob.peer_id.clone(), "once")
        .await
        .unwrap();
    assert_eq!(bob.incoming.recv().await.unwrap().body, "once");
    handle.wait().await.unwrap();

    alice.transport.replace_contacts([]).await.unwrap();
    assert!(matches!(
        alice.client.send_text(bob.peer_id.clone(), "twice").await,
        Err(DeliveryError::NotAContact)
    ));

    alice.transport.shutdown().await.unwrap();
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn oversized_declared_length_produces_no_incoming_message() {
    let alice_secret = SecretKey::from_bytes(&[71; 32]);
    let bob_secret = SecretKey::from_bytes(&[72; 32]);
    let alice_id = peer_id_from_secret(&alice_secret);
    let mut bob = local_peer(bob_secret, [alice_id]).await;

    let alice = Endpoint::builder(presets::N0)
        .secret_key(alice_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();

    let bob_addr = direct_loopback_addr(&bob.transport).await;
    let connection = alice.connect(bob_addr, CHAT_ALPN).await.unwrap();
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    let oversized = ((MAX_FRAME_BYTES as u32) + 1).to_be_bytes();
    tokio::io::AsyncWriteExt::write_all(&mut send, &oversized)
        .await
        .unwrap();
    let _ = send.finish();
    assert_invalid_stream_has_no_receipt(&mut recv).await;

    assert!(
        tokio::time::timeout(Duration::from_millis(200), bob.incoming.recv())
            .await
            .is_err()
    );

    alice.close().await;
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_and_trailing_inbound_streams_reset_without_receipts() {
    let alice_secret = SecretKey::from_bytes(&[73; 32]);
    let bob_secret = SecretKey::from_bytes(&[74; 32]);
    let alice_id = peer_id_from_secret(&alice_secret);
    let mut bob = local_peer(bob_secret, [alice_id]).await;
    let alice = Endpoint::builder(presets::N0)
        .secret_key(alice_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    let connection = alice
        .connect(direct_loopback_addr(&bob.transport).await, CHAT_ALPN)
        .await
        .unwrap();

    let (mut malformed_send, mut malformed_recv) = connection.open_bi().await.unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut malformed_send, &[0, 0, 0, 1, 0xff])
        .await
        .unwrap();
    malformed_send.finish().unwrap();
    assert_invalid_stream_has_no_receipt(&mut malformed_recv).await;

    let (mut trailing_send, mut trailing_recv) = connection.open_bi().await.unwrap();
    write_document(
        &mut trailing_send,
        &WireEnvelope::new(ChatFrame::text(MessageId::new([75; 16]), 1, "first").unwrap()),
    )
    .await
    .unwrap();
    tokio::io::AsyncWriteExt::write_all(&mut trailing_send, b"trailing")
        .await
        .unwrap();
    trailing_send.finish().unwrap();
    assert_invalid_stream_has_no_receipt(&mut trailing_recv).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), bob.incoming.recv())
            .await
            .is_err(),
        "invalid streams must not publish IncomingText"
    );

    // A valid stream following both resets demonstrates that only each stream,
    // rather than the accepted connection, was terminated.
    let message_id = MessageId::new([76; 16]);
    let (mut valid_send, mut valid_recv) = connection.open_bi().await.unwrap();
    write_document(
        &mut valid_send,
        &WireEnvelope::new(ChatFrame::text(message_id, 2, "after reset").unwrap()),
    )
    .await
    .unwrap();
    valid_send.finish().unwrap();
    let incoming = tokio::time::timeout(Duration::from_secs(2), bob.incoming.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(incoming.message_id, message_id);
    let receipt = read_document(&mut valid_recv).await.unwrap();
    assert!(matches!(
        receipt.frame,
        ChatFrame::Accepted {
            message_id: received_id,
            ..
        } if received_id == message_id
    ));

    alice.close().await;
    bob.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn wrong_receipt_frame_resets_only_the_stream_and_keeps_the_session() {
    let bob_secret = SecretKey::from_bytes(&[82; 32]);
    let alice_secret = SecretKey::from_bytes(&[81; 32]);
    let bob_peer_id = peer_id_from_secret(&bob_secret);

    let bob = Endpoint::builder(presets::N0)
        .secret_key(bob_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    let bob_endpoint_id = bob.id();

    let accept = {
        let bob = bob.clone();
        tokio::spawn(async move {
            let incoming = bob.accept().await.unwrap();
            let connection = incoming.await.unwrap();
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let envelope = read_document(&mut recv).await.unwrap();
            assert!(matches!(envelope.frame, ChatFrame::Text { .. }));
            // An Accepted receipt for another delivery violates the exchange
            // correlation contract and must reset only this bidi stream.
            let bogus = WireEnvelope::new(ChatFrame::accepted(MessageId::new([9; 16]), 0));
            write_document(&mut send, &bogus).await.unwrap();
            send.finish().unwrap();
            // Keep the connection alive until the peer has read the reply.
            let _ = send.stopped().await;
            let (mut send, mut recv) = connection.accept_bi().await.unwrap();
            let envelope = read_document(&mut recv).await.unwrap();
            let ChatFrame::Text { message_id, .. } = envelope.frame else {
                panic!("expected text");
            };
            let accepted = WireEnvelope::new(ChatFrame::accepted(message_id, 1));
            write_document(&mut send, &accepted).await.unwrap();
            send.finish().unwrap();
            let _ = send.stopped().await;
            connection
        })
    };

    let alice = local_peer(alice_secret, []).await;
    let mut bob_addr = bob.addr();
    bob_addr.addrs.retain(|transport_addr| {
        matches!(
            transport_addr,
            TransportAddr::Ip(socket) if socket.ip().is_loopback()
        )
    });
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while bob_addr.ip_addrs().next().is_none() {
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(20)).await;
        bob_addr = bob.addr();
        bob_addr.addrs.retain(|transport_addr| {
            matches!(
                transport_addr,
                TransportAddr::Ip(socket) if socket.ip().is_loopback()
            )
        });
    }
    set_test_route(alice.transport.inner(), bob_endpoint_id, bob_addr).await;
    alice
        .transport
        .replace_contacts([bob_peer_id.clone()])
        .await
        .unwrap();
    wait_for_peer_state(&alice, &bob_peer_id, ContactConnectionState::Connected).await;

    let result = alice
        .client
        .send_text(bob_peer_id.clone(), "hello")
        .await
        .unwrap()
        .wait()
        .await;
    assert!(
        matches!(result, Err(DeliveryError::ProtocolViolation)),
        "unexpected delivery result: {result:?}"
    );
    let second = alice
        .client
        .send_text(bob_peer_id, "again")
        .await
        .unwrap()
        .wait()
        .await;
    assert!(second.is_ok());
    accept.await.unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.close().await;
}

#[tokio::test]
async fn removing_contact_cancels_queued_outbound_deliveries() {
    let alice_secret = SecretKey::from_bytes(&[83; 32]);
    let bob_secret = SecretKey::from_bytes(&[84; 32]);
    let bob_peer_id = peer_id_from_secret(&bob_secret);
    let bob_endpoint_id = bob_secret.public();

    let alice = local_peer(alice_secret, []).await;
    let bob = Endpoint::builder(presets::N0)
        .secret_key(bob_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    set_test_route(
        alice.transport.inner(),
        bob_endpoint_id,
        direct_loopback_endpoint_addr(&bob).await,
    )
    .await;

    let (first_delivery_seen_tx, first_delivery_seen_rx) = oneshot::channel();
    let (release_blocked_stream_tx, release_blocked_stream_rx) = oneshot::channel::<()>();
    let accept_first = {
        let bob = bob.clone();
        tokio::spawn(async move {
            let incoming = bob.accept().await.expect("first incoming connection");
            let connection = incoming.await.expect("first connection");
            let (_send, mut recv) = connection.accept_bi().await.expect("first bidi stream");
            let envelope = read_document(&mut recv).await.expect("first text frame");
            assert!(matches!(envelope.frame, ChatFrame::Text { .. }));
            let _ = first_delivery_seen_tx.send(());
            let _ = release_blocked_stream_rx.await;
            connection.close(0u32.into(), b"test done");
        })
    };

    alice
        .transport
        .replace_contacts([bob_peer_id.clone()])
        .await
        .unwrap();
    wait_for_peer_state(&alice, &bob_peer_id, ContactConnectionState::Connected).await;

    let blocked = alice
        .client
        .send_text(bob_peer_id.clone(), "blocked-first")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), first_delivery_seen_rx)
        .await
        .expect("first delivery should reach the remote peer")
        .unwrap();

    let mut queued = Vec::with_capacity(OUTGOING_QUEUE_CAPACITY);
    for index in 0..OUTGOING_QUEUE_CAPACITY {
        queued.push(
            alice
                .client
                .send_text(bob_peer_id.clone(), format!("queued-{index}"))
                .await
                .unwrap(),
        );
    }

    alice.transport.replace_contacts([]).await.unwrap();

    for handle in queued {
        assert!(matches!(
            handle.wait().await,
            Err(DeliveryError::NotAContact)
        ));
    }
    assert!(
        blocked.wait().await.is_err(),
        "revoking a contact should also settle the in-flight delivery"
    );
    assert!(matches!(
        alice
            .client
            .send_text(bob_peer_id.clone(), "blocked-next")
            .await,
        Err(DeliveryError::NotAContact)
    ));

    let _ = release_blocked_stream_tx.send(());
    accept_first.await.unwrap();

    alice.transport.shutdown().await.unwrap();
    bob.close().await;
}

#[tokio::test]
async fn outbound_queue_full_returns_immediately() {
    let alice_secret = SecretKey::from_bytes(&[85; 32]);
    let bob_secret = SecretKey::from_bytes(&[86; 32]);
    let bob_peer_id = peer_id_from_secret(&bob_secret);
    let bob_endpoint_id = bob_secret.public();

    let alice = local_peer(alice_secret, []).await;
    let bob = Endpoint::builder(presets::N0)
        .secret_key(bob_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();
    set_test_route(
        alice.transport.inner(),
        bob_endpoint_id,
        direct_loopback_endpoint_addr(&bob).await,
    )
    .await;

    let (first_delivery_seen_tx, first_delivery_seen_rx) = oneshot::channel();
    let (release_blocked_stream_tx, release_blocked_stream_rx) = oneshot::channel::<()>();
    let accept_first = {
        let bob = bob.clone();
        tokio::spawn(async move {
            let incoming = bob.accept().await.expect("incoming connection");
            let connection = incoming.await.expect("connection");
            let (_send, mut recv) = connection.accept_bi().await.expect("bidi stream");
            let _ = read_document(&mut recv).await.expect("text frame");
            let _ = first_delivery_seen_tx.send(());
            let _ = release_blocked_stream_rx.await;
            connection.close(0u32.into(), b"test done");
        })
    };

    alice
        .transport
        .replace_contacts([bob_peer_id.clone()])
        .await
        .unwrap();
    wait_for_peer_state(&alice, &bob_peer_id, ContactConnectionState::Connected).await;

    alice
        .client
        .send_text(bob_peer_id.clone(), "active")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), first_delivery_seen_rx)
        .await
        .expect("active delivery should reach remote")
        .unwrap();

    for index in 0..OUTGOING_QUEUE_CAPACITY {
        alice
            .client
            .send_text(bob_peer_id.clone(), format!("fill-out-{index}"))
            .await
            .unwrap();
    }

    assert!(matches!(
        alice
            .client
            .send_text(bob_peer_id.clone(), "overflow")
            .await,
        Err(DeliveryError::QueueFull)
    ));

    let _ = release_blocked_stream_tx.send(());
    accept_first.await.unwrap();
    alice.transport.shutdown().await.unwrap();
    bob.close().await;
}

#[tokio::test]
async fn initial_dial_timeout_becomes_not_connected_without_retry() {
    let alice_secret = SecretKey::from_bytes(&[93; 32]);
    let bob_secret = SecretKey::from_bytes(&[94; 32]);
    let bob_id = peer_id_from_secret(&bob_secret);
    let bob_endpoint_id = bob_secret.public();

    let alice = local_peer_with_config(alice_secret, [], ChatTransportConfig::default()).await;

    let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_addr = dead_listener.local_addr().unwrap();
    drop(dead_listener);

    let unreachable = EndpointAddr::from_parts(bob_endpoint_id, vec![TransportAddr::Ip(dead_addr)]);
    set_test_route(alice.transport.inner(), bob_endpoint_id, unreachable).await;
    alice
        .transport
        .replace_contacts([bob_id.clone()])
        .await
        .unwrap();
    wait_for_peer_state(&alice, &bob_id, ContactConnectionState::NotConnected).await;

    let attempts = alice.transport.dial_attempt_count();
    assert_eq!(attempts, 1);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(alice.transport.dial_attempt_count(), attempts);
    assert!(matches!(
        alice.client.send_text(bob_id, "stuck").await,
        Err(DeliveryError::PeerNotConnected)
    ));

    alice.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn outbound_dials_are_bounded_by_shared_semaphore() {
    use crate::network::identity::peer_id_to_endpoint_id;

    let alice_secret = SecretKey::from_bytes(&[99; 32]);
    let alice = local_peer_with_config(alice_secret, [], ChatTransportConfig::default()).await;
    let contact_count = MAX_OUTBOUND_DIALS + 4;
    let mut contacts = Vec::with_capacity(contact_count);
    for index in 0..contact_count {
        let mut bytes = [110_u8; 32];
        bytes[0] = index as u8;
        let peer = peer_id_from_secret(&SecretKey::from_bytes(&bytes));
        let endpoint_id = peer_id_to_endpoint_id(&peer).unwrap();
        contacts.push(peer);
        let dead_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = dead_listener.local_addr().unwrap();
        drop(dead_listener);
        set_test_route(
            alice.transport.inner(),
            endpoint_id,
            EndpointAddr::from_parts(endpoint_id, vec![TransportAddr::Ip(dead_addr)]),
        )
        .await;
    }

    alice
        .transport
        .replace_contacts(contacts.clone())
        .await
        .unwrap();
    for peer in &contacts {
        wait_for_peer_state(&alice, peer, ContactConnectionState::NotConnected).await;
    }
    assert!(alice.transport.dial_peak_occupancy() <= MAX_OUTBOUND_DIALS);
    assert_eq!(alice.transport.dial_attempt_count(), contact_count);

    alice.transport.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_session_budget_rejects_extra_connections() {
    let intruder_secret = SecretKey::from_bytes(&[96; 32]);
    let intruder_id = peer_id_from_secret(&intruder_secret);
    let alice = local_peer(SecretKey::from_bytes(&[95; 32]), [intruder_id]).await;
    alice
        .transport
        .drop_session_for_test(intruder_secret.public())
        .await;
    let alice_addr = direct_loopback_addr(&alice.transport).await;
    let _budget = exhaust_inbound_session_budget(alice.transport.inner()).await;
    assert_eq!(inbound_session_budget(alice.transport.inner()), 0);

    let intruder = Endpoint::builder(presets::N0)
        .secret_key(intruder_secret)
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();

    let connection = intruder.connect(alice_addr, CHAT_ALPN).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        connection.open_bi().await.is_err(),
        "server should close connections when the inbound session budget is exhausted"
    );

    alice.transport.shutdown().await.unwrap();
    intruder.close().await;
}

#[tokio::test]
async fn inbound_handler_budget_rejects_connections_before_handshake() {
    let alice = local_peer(SecretKey::from_bytes(&[97; 32]), []).await;
    let alice_addr = direct_loopback_addr(&alice.transport).await;
    let _budget = exhaust_inbound_handler_budget(alice.transport.inner()).await;
    assert_eq!(inbound_handler_budget(alice.transport.inner()), 0);

    let intruder = Endpoint::builder(presets::N0)
        .secret_key(SecretKey::from_bytes(&[98; 32]))
        .alpns(vec![CHAT_ALPN.to_vec()])
        .relay_mode(iroh::RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .bind()
        .await
        .unwrap();

    let result = tokio::time::timeout(
        Duration::from_secs(2),
        intruder.connect(alice_addr, CHAT_ALPN),
    )
    .await
    .expect("connection refusal should not hang")
    .map(|_| ())
    .is_err();
    assert!(
        result,
        "server should refuse connections when the inbound handler budget is exhausted"
    );

    alice.transport.shutdown().await.unwrap();
    intruder.close().await;
}
