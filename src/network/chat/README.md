# Rathole Iroh Chat Transport

This module adapts the standalone Rathole wire protocol to authenticated Iroh connections. It owns peer sessions, connection lifecycle, stream framing, delivery admission, and runtime connection state. It does not own the CBOR schema; that contract is documented in [`src/protocol/README.md`](../../protocol/README.md).

## Internal architecture

`SessionActor` owns the lifecycle of each peer: queueing, delivery completion,
connection selection, primary/draining transitions, path diagnostics, and
shutdown. The delivery policy owns one complete message exchange, including
framing, receipt validation, authorization ordering, deadlines, cancellation,
and stream-reset decisions. The Iroh adapter owns only opened-stream primitives,
including Iroh send completion and best-effort reset/stop operations. This
separation keeps a failed or malformed stream local to that stream rather than
turning it into a peer-session connection failure.

## Transport boundary

- The chat endpoint uses the versioned ALPN `rathole/chat/1`.
- One `ChatTransport` is started for a TUI session and is closed when that session exits.
- The transport creates one `PeerSession` per locally stored contact.
- Incoming traffic is authorised from the authenticated Iroh `EndpointId`; an identity field inside CBOR is not trusted.
- A contact may receive traffic only while it is in the local contact allowlist.

`Connected` means that a local Iroh/QUIC connection has completed its handshake. It is not remote presence. The configured `RATHOLE_IROH_PATH_MODE` (`auto` or `relay-only`) controls path selection policy; it is not a report of the path currently selected by Iroh.

A relay-backed connection is immediately usable for message delivery once the handshake completes. Direct IP is an opportunistic optimization that Iroh may select later in `auto` mode. Selected-path diagnostics describe the transport path Iroh currently prefers; they do not gate message admission or delivery readiness.

## Peer-session lifecycle

On startup and after adding a contact, the transport makes one outbound dial attempt for that contact. The external state is:

```text
Connecting -> Connected
     \-> Not connected
```

After a failed initial dial there is no polling loop or automatic retry. A later inbound dial from the remote peer can still attach to the local peer session and move it to `Connected`. Sending is rejected while a contact is `Connecting` or `Not connected`.

Removing a contact closes its peer session and cancels active and queued deliveries with `NotAContact`. Removal is blocked while the contact is still `Connecting`.

## Connections and streams

When a contact is reachable, its peer session owns one long-lived Iroh connection. Each message opens a new bidirectional stream and performs exactly one request-response exchange:

```text
Text -> Accepted | Rejected
```

The connection is reused across messages and idle periods. Independent streams may be active in both directions at once. If both peers dial simultaneously, a deterministic local/remote `EndpointId` rule keeps one preferred connection. A stream-level protocol error resets that stream without evicting a healthy connection.

The transport uses the authenticated connection for peer identity and the stream for one complete, already-delimited protocol document. It does not add identity, presence, ordering, or persistence fields to the CBOR envelope.

## Framing and bounds

Each stream document is preceded by a four-byte big-endian length prefix. The declared CBOR document and the encoded protocol document are capped at 32 KiB. A stream containing trailing bytes after its one document is rejected.

Current runtime bounds are deliberately finite:

- per-peer incoming and outgoing message queues: 64 messages;
- inbound peer sessions: 64;
- inbound stream handlers: 64;
- concurrent outbound dials: 8;
- inbound stream operation timeout: 5 seconds;
- initial contact dial timeout: 5 seconds in production;
- local delivery deadline: 30 seconds in production, including queueing and stream I/O.

Tests use shorter dial and delivery deadlines where necessary to keep negative-path tests bounded; those test budgets are not user-facing runtime behavior.

A full per-peer outgoing queue returns `QueueFull` immediately instead of blocking the caller. Outgoing messages are attempted FIFO per peer.

## Delivery semantics

`send_text` admits only a valid message addressed to a local contact whose session is connected. A successful `DeliveryHandle` completes after the remote Rathole runtime returns `Accepted`. `Rejected`, queue overflow, cancellation, timeout, protocol failure, transport failure, and shutdown remain distinct error outcomes.

The transport is online-only. It does not persist messages, retry unknown delivery outcomes, deduplicate messages, assign sequence numbers, provide an offline mailbox, or claim that a message was read.

For cross-machine troubleshooting, `message_id` and `stream_id` can be correlated between logs. `connection_id` is local to one process and must not be treated as a globally shared identifier. See [`docs/diagnostics.md`](../../../docs/diagnostics.md).
