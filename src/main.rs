//! pNet Chat — Phase 1 skeleton.
//!
//! Proves the four app-API ops end to end through the chat protocol envelope:
//! register (alias `pnet-chat`) → get-data → send → push. There is no room,
//! member, UA, or RH logic yet (Phases 2+); this binary only establishes the
//! pipe and the `[version][msg_type][room_id:16][body]` framing every later
//! message rides on. The HTTP UI sends a dev test frame (`DEV_TEXT`) to any
//! destination and logs inbound frames decoded by `msg_type`.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json, Router,
    extract::State,
    response::Html,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tokio::net::UdpSocket;

// ── Constants ─────────────────────────────────────────────────────────────────

const PNET_ADDR_DEFAULT: &str = "127.0.0.1:7777";
const TOKEN_FILE_DEFAULT: &str = "pnet_chat_token.bin";
/// Port that pnet pushes received app packets to (registered with pnet).
const PUSH_PORT_DEFAULT: u16 = 8890;
/// Port used for control requests (register, get-data) and their replies.
/// Separate from PUSH_PORT so the background push loop never races with replies.
const CTRL_PORT_DEFAULT: u16 = 8891;
const HTTP_PORT_DEFAULT: u16 = 3100;
/// The well-known alias every install of this app registers under. Discovery is
/// by alias (the per-device `app_id` is a fresh uuid per registration, so it is
/// not a shared constant) — see description.md, *Discovery and addressing*.
const APP_ALIAS: &str = "pnet-chat";
const APP_PROTOCOL: &str = "application/pnet-chat";

// pnet op bytes
const OP_REGISTER: u8 = 0x00;
const OP_GET_DATA: u8 = 0x02;
const OP_SEND: u8 = 0x03;
const OP_PUSH: u8 = 0x04;
const STATUS_OK: u8 = 0x00;

// grade byte in the get-data response (src/lib/handlers.rs encode_device)
const GRADE_SG: u8 = 1;

// ── Chat protocol envelope ────────────────────────────────────────────────────
//
// Every app payload begins with `[version:u8][msg_type:u8][room_id:16][body]`.
// See description.md, *App-level protocol*. Phase 1 implements only the framing
// plus a dev test frame; the spec `msg_type`s are declared here for forward
// reference and so the dispatcher can name what it does not yet handle.

const PROTO_VERSION: u8 = 1;
const ROOM_ID_LEN: usize = 16;
const ENVELOPE_LEN: usize = 1 + 1 + ROOM_ID_LEN; // version + msg_type + room_id
/// Single-datagram budget for an app payload, envelope included (description.md).
const MAX_APP_PAYLOAD: usize = 1024;

mod msg {
    // Control and text (Phases 3–7).
    pub const OPEN_ROOM: u8 = 0x01;
    pub const ROOM_CREATED: u8 = 0x02;
    pub const INVITE: u8 = 0x03;
    pub const HELLO: u8 = 0x04;
    pub const ADD_MEMBER: u8 = 0x06;
    pub const REMOVE_MEMBER: u8 = 0x07;
    pub const LEAVE: u8 = 0x08;
    pub const MEMBER_UPDATE: u8 = 0x09;
    pub const POST: u8 = 0x0A;
    pub const MSG: u8 = 0x0B;
    pub const ACK: u8 = 0x0C;
    pub const HISTORY_REQ: u8 = 0x0D;
    pub const HISTORY_RESP: u8 = 0x0E;
    pub const HOST_MOVED: u8 = 0x0F;
    // Intra-user, client (DG) ↔ User Agent (own top SG). Phase 2.
    pub const CLIENT_ATTACH: u8 = 0x10;
    pub const CLIENT_ATTACH_ACK: u8 = 0x11;
    // Media (Phases 8–9).
    pub const MEDIA_JOIN: u8 = 0x20;
    pub const MEDIA_LEAVE: u8 = 0x21;
    pub const MEDIA_FRAME: u8 = 0x22;
    // Blob family (Phase 6).
    pub const BLOB_OFFER: u8 = 0x30;
    pub const BLOB_CHUNK: u8 = 0x31;
    pub const BLOB_ACK: u8 = 0x32;
    pub const BLOB_NACK: u8 = 0x33;
    /// Phase-1-only test vehicle: a raw text frame for exercising the pipe.
    /// Not part of the real protocol; lives in the dev/experimental range.
    pub const DEV_TEXT: u8 = 0xF0;

    /// Human-readable name for a `msg_type`, for logging unhandled frames.
    pub fn name(t: u8) -> &'static str {
        match t {
            OPEN_ROOM => "OPEN_ROOM",
            ROOM_CREATED => "ROOM_CREATED",
            INVITE => "INVITE",
            HELLO => "HELLO",
            ADD_MEMBER => "ADD_MEMBER",
            REMOVE_MEMBER => "REMOVE_MEMBER",
            LEAVE => "LEAVE",
            MEMBER_UPDATE => "MEMBER_UPDATE",
            POST => "POST",
            MSG => "MSG",
            ACK => "ACK",
            HISTORY_REQ => "HISTORY_REQ",
            HISTORY_RESP => "HISTORY_RESP",
            HOST_MOVED => "HOST_MOVED",
            CLIENT_ATTACH => "CLIENT_ATTACH",
            CLIENT_ATTACH_ACK => "CLIENT_ATTACH_ACK",
            MEDIA_JOIN => "MEDIA_JOIN",
            MEDIA_LEAVE => "MEDIA_LEAVE",
            MEDIA_FRAME => "MEDIA_FRAME",
            BLOB_OFFER => "BLOB_OFFER",
            BLOB_CHUNK => "BLOB_CHUNK",
            BLOB_ACK => "BLOB_ACK",
            BLOB_NACK => "BLOB_NACK",
            DEV_TEXT => "DEV_TEXT",
            _ => "UNKNOWN",
        }
    }
}

const ZERO_ROOM: [u8; ROOM_ID_LEN] = [0u8; ROOM_ID_LEN];

/// Encode `[version][msg_type][room_id][body]`.
fn encode_envelope(msg_type: u8, room_id: &[u8; ROOM_ID_LEN], body: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(ENVELOPE_LEN + body.len());
    buf.push(PROTO_VERSION);
    buf.push(msg_type);
    buf.extend_from_slice(room_id);
    buf.extend_from_slice(body);
    buf
}

struct Envelope<'a> {
    msg_type: u8,
    room_id: [u8; ROOM_ID_LEN],
    body: &'a [u8],
}

/// Decode an envelope. Returns None on a short payload or an unknown version
/// (forward-compatibility: a future version is dropped, not misparsed).
fn decode_envelope(data: &[u8]) -> Option<Envelope<'_>> {
    if data.len() < ENVELOPE_LEN || data[0] != PROTO_VERSION {
        return None;
    }
    let room_id: [u8; ROOM_ID_LEN] = data[2..2 + ROOM_ID_LEN].try_into().unwrap();
    Some(Envelope {
        msg_type: data[1],
        room_id,
        body: &data[ENVELOPE_LEN..],
    })
}

/// Build a `DEV_TEXT` body: `[len:u16][utf8]`.
fn encode_dev_text(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut body = Vec::with_capacity(2 + bytes.len());
    body.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    body.extend_from_slice(bytes);
    body
}

fn decode_dev_text(body: &[u8]) -> Option<String> {
    let len = u16::from_be_bytes([*body.get(0)?, *body.get(1)?]) as usize;
    let s = std::str::from_utf8(body.get(2..2 + len)?).ok()?;
    Some(s.to_string())
}

// ── Room-control bodies (description.md, *App-level protocol*) ─────────────────

/// One enveloped frame to send to `(dest_device, dest_app)`. Collected while
/// holding the state lock, flushed after releasing it.
struct Outbound {
    dest_device: [u8; 16],
    dest_app: [u8; 16],
    msg_type: u8,
    room_id: [u8; 16],
    body: Vec<u8>,
}

/// `OPEN_ROOM` body: `name:str, retention:u8, join_hist:u8, member_count:u8,
/// member_user:16 ×N`.
fn encode_open_room(name: &str, retention: u8, join_hist: u8, members: &[[u8; 16]]) -> Vec<u8> {
    let mut b = Vec::new();
    push_str(&mut b, name);
    b.push(retention);
    b.push(join_hist);
    b.push(members.len() as u8);
    for m in members { b.extend_from_slice(m); }
    b
}

struct OpenRoom {
    name: String,
    retention: u8,
    join_hist: u8,
    members: Vec<[u8; 16]>,
}

fn decode_open_room(body: &[u8]) -> Option<OpenRoom> {
    let mut pos = 0;
    let name = read_str(body, &mut pos)?;
    let retention = *body.get(pos)?; pos += 1;
    let join_hist = *body.get(pos)?; pos += 1;
    let count = *body.get(pos)? as usize; pos += 1;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        members.push(read_bytes::<16>(body, &mut pos)?);
    }
    Some(OpenRoom { name, retention, join_hist, members })
}

/// `INVITE` body: `name:str, host_user:16, rh_device:16, rh_app_id:16,
/// retention:u8, join_hist:u8, member_count:u8, (member_user:16, alias:str)×N`.
fn encode_invite(
    name: &str, host_user: &[u8; 16], rh_device: &[u8; 16], rh_app: &[u8; 16],
    retention: u8, join_hist: u8, members: &[Member],
) -> Vec<u8> {
    let mut b = Vec::new();
    push_str(&mut b, name);
    b.extend_from_slice(host_user);
    b.extend_from_slice(rh_device);
    b.extend_from_slice(rh_app);
    b.push(retention);
    b.push(join_hist);
    b.push(members.len() as u8);
    for m in members {
        b.extend_from_slice(&m.user_uuid);
        push_str(&mut b, &m.alias);
    }
    b
}

struct Invite {
    name: String,
    host_user: [u8; 16],
    rh_device: [u8; 16],
    rh_app: [u8; 16],
    retention: u8,
    join_hist: u8,
    members: Vec<([u8; 16], String)>,
}

fn decode_invite(body: &[u8]) -> Option<Invite> {
    let mut pos = 0;
    let name = read_str(body, &mut pos)?;
    let host_user = read_bytes::<16>(body, &mut pos)?;
    let rh_device = read_bytes::<16>(body, &mut pos)?;
    let rh_app = read_bytes::<16>(body, &mut pos)?;
    let retention = *body.get(pos)?; pos += 1;
    let join_hist = *body.get(pos)?; pos += 1;
    let count = *body.get(pos)? as usize; pos += 1;
    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        let u = read_bytes::<16>(body, &mut pos)?;
        let a = read_str(body, &mut pos)?;
        members.push((u, a));
    }
    Some(Invite { name, host_user, rh_device, rh_app, retention, join_hist, members })
}

/// `HELLO` body: `ua_device:16, ua_app_id:16, room_count:u16,
/// (room_id:16, last_seq:u64)×N`.
fn encode_hello(ua_device: &[u8; 16], ua_app: &[u8; 16], rooms: &[([u8; 16], u64)]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(ua_device);
    b.extend_from_slice(ua_app);
    b.extend_from_slice(&(rooms.len() as u16).to_be_bytes());
    for (rid, seq) in rooms {
        b.extend_from_slice(rid);
        b.extend_from_slice(&seq.to_be_bytes());
    }
    b
}

fn decode_hello(body: &[u8]) -> Option<(/*device*/[u8; 16], /*app*/[u8; 16], Vec<([u8; 16], u64)>)> {
    let mut pos = 0;
    let device = read_bytes::<16>(body, &mut pos)?;
    let app = read_bytes::<16>(body, &mut pos)?;
    let count = u16::from_be_bytes([*body.get(pos)?, *body.get(pos + 1)?]) as usize;
    pos += 2;
    let mut rooms = Vec::with_capacity(count);
    for _ in 0..count {
        let rid = read_bytes::<16>(body, &mut pos)?;
        let seq_bytes = read_bytes::<8>(body, &mut pos)?;
        rooms.push((rid, u64::from_be_bytes(seq_bytes)));
    }
    Some((device, app, rooms))
}

/// `MEMBER_UPDATE` body: `change:u8 (0=add,1=remove,2=leave), member_user:16, alias:str`.
const MEMBER_CHANGE_ADD: u8 = 0;
const MEMBER_CHANGE_REMOVE: u8 = 1;
const MEMBER_CHANGE_LEAVE: u8 = 2;

fn encode_member_update(change: u8, user: &[u8; 16], alias: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(change);
    b.extend_from_slice(user);
    push_str(&mut b, alias);
    b
}

fn decode_member_update(body: &[u8]) -> Option<(u8, [u8; 16], String)> {
    let mut pos = 0;
    let change = *body.get(pos)?; pos += 1;
    let user = read_bytes::<16>(body, &mut pos)?;
    let alias = read_str(body, &mut pos)?;
    Some((change, user, alias))
}

fn push_text(buf: &mut Vec<u8>, text: &str) {
    let b = text.as_bytes();
    buf.extend_from_slice(&(b.len() as u16).to_be_bytes());
    buf.extend_from_slice(b);
}

fn read_text(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = u16::from_be_bytes([*data.get(*pos)?, *data.get(*pos + 1)?]) as usize;
    *pos += 2;
    let s = std::str::from_utf8(data.get(*pos..*pos + len)?).ok()?.to_string();
    *pos += len;
    Some(s)
}

/// `POST` body: `client_msg_id:16, text, attach_count:u8` (attachments are
/// Phase 6 — always 0 for now).
fn encode_post(client_msg_id: &[u8; 16], text: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(client_msg_id);
    push_text(&mut b, text);
    b.push(0); // attach_count
    b
}

fn decode_post(body: &[u8]) -> Option<([u8; 16], String)> {
    let mut pos = 0;
    let cid = read_bytes::<16>(body, &mut pos)?;
    let text = read_text(body, &mut pos)?;
    // attach_count present but ignored until Phase 6.
    Some((cid, text))
}

/// `MSG` body: `seq:u64, sender_user:16, ts_ms:u64, client_msg_id:16, text,
/// attach_count:u8`.
fn encode_msg(seq: u64, sender_user: &[u8; 16], ts_ms: u64, client_msg_id: &[u8; 16], text: &str) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&seq.to_be_bytes());
    b.extend_from_slice(sender_user);
    b.extend_from_slice(&ts_ms.to_be_bytes());
    b.extend_from_slice(client_msg_id);
    push_text(&mut b, text);
    b.push(0); // attach_count
    b
}

struct DecodedMsg {
    seq: u64,
    sender_user: [u8; 16],
    ts_ms: u64,
    #[allow(dead_code)]
    client_msg_id: [u8; 16],
    text: String,
}

fn decode_msg(body: &[u8]) -> Option<DecodedMsg> {
    let mut pos = 0;
    let seq = u64::from_be_bytes(read_bytes::<8>(body, &mut pos)?);
    let sender_user = read_bytes::<16>(body, &mut pos)?;
    let ts_ms = u64::from_be_bytes(read_bytes::<8>(body, &mut pos)?);
    let client_msg_id = read_bytes::<16>(body, &mut pos)?;
    let text = read_text(body, &mut pos)?;
    Some(DecodedMsg { seq, sender_user, ts_ms, client_msg_id, text })
}

/// `ACK` body: `seq:u64` (cumulative highest-contiguous applied).
fn encode_ack(seq: u64) -> Vec<u8> { seq.to_be_bytes().to_vec() }
fn decode_ack(body: &[u8]) -> Option<u64> {
    let mut pos = 0;
    Some(u64::from_be_bytes(read_bytes::<8>(body, &mut pos)?))
}

/// `HISTORY_REQ` body: `since_seq:u64, max_count:u16`.
fn encode_history_req(since: u64, max: u16) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&since.to_be_bytes());
    b.extend_from_slice(&max.to_be_bytes());
    b
}
fn decode_history_req(body: &[u8]) -> Option<(u64, u16)> {
    let mut pos = 0;
    let since = u64::from_be_bytes(read_bytes::<8>(body, &mut pos)?);
    let max = u16::from_be_bytes(read_bytes::<2>(body, &mut pos)?);
    Some((since, max))
}

/// `HISTORY_RESP` body: `from_seq:u64, to_seq:u64, more:u8, count:u16,
/// (entry_len:u16, MSG-body)×count`. Each entry is a full MSG body so the
/// requester applies it exactly like a live MSG. Self-chunking: while `more`,
/// the requester re-asks with `since = to_seq`.
fn encode_history_resp(from: u64, to: u64, more: bool, entries: &[Vec<u8>]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&from.to_be_bytes());
    b.extend_from_slice(&to.to_be_bytes());
    b.push(more as u8);
    b.extend_from_slice(&(entries.len() as u16).to_be_bytes());
    for e in entries {
        b.extend_from_slice(&(e.len() as u16).to_be_bytes());
        b.extend_from_slice(e);
    }
    b
}
fn decode_history_resp(body: &[u8]) -> Option<(u64, u64, bool, Vec<Vec<u8>>)> {
    let mut pos = 0;
    let from = u64::from_be_bytes(read_bytes::<8>(body, &mut pos)?);
    let to = u64::from_be_bytes(read_bytes::<8>(body, &mut pos)?);
    let more = *body.get(pos)? != 0; pos += 1;
    let count = u16::from_be_bytes(read_bytes::<2>(body, &mut pos)?) as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let len = u16::from_be_bytes(read_bytes::<2>(body, &mut pos)?) as usize;
        let e = body.get(pos..pos + len)?.to_vec();
        pos += len;
        entries.push(e);
    }
    Some((from, to, more, entries))
}

// ── Data types ────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize)]
struct AppInfo {
    /// 32-char lowercase hex of the 16-byte app uuid.
    id_hex: String,
    alias: String,
    approved: bool,
}

#[derive(Clone, Debug, Serialize)]
struct Destination {
    label: String,
    /// Owner or contact user uuid this device belongs to.
    user_uuid_hex: String,
    device_uuid: Vec<u8>,
    /// "SG" or "DG".
    grade: String,
    sg_rank: u8,
    /// True if this device belongs to our own user (vs a contact).
    own_user: bool,
    app_id_hex: String,
    #[serde(skip)]
    app_id: [u8; 16],
}

/// This node's role for its own user, derived from the get-data tree.
/// See description.md, *Roles and topology*.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
enum Role {
    /// This device is its user's top-ranked SG — the acting User Agent (hub).
    UserAgent,
    /// An SG, but not the top-ranked one — a UA mirror/standby (Phase 7).
    SgStandby,
    /// A DG (thin client) — delegates to its own user's UA.
    DataGuest,
    /// Not yet known (no successful get-data, or local device not found).
    Unknown,
}

/// A resolved address for the local user's User Agent — its top-ranked SG
/// device and the `pnet-chat` app on it. `None` app_id means the UA device is
/// known but its app has not synced into our snapshot yet (tolerate staleness).
#[derive(Clone, Debug, Serialize)]
struct UaRef {
    device_uuid_hex: String,
    #[serde(skip)]
    device_uuid: [u8; 16],
    label: String,
    sg_rank: u8,
    app_id_hex: Option<String>,
    #[serde(skip)]
    app_id: Option<[u8; 16]>,
    /// True when this UA is the local node itself.
    is_self: bool,
}

/// One user's devices, as seen in our get-data snapshot — used to resolve any
/// user's User Agent (their top-ranked SG running pnet-chat) for INVITE/routing.
#[derive(Clone, Debug)]
struct UserDir {
    alias: String,
    own: bool,
    /// (device_uuid, sg_rank, chat_app_id) for each SG device that runs the app.
    sgs: Vec<([u8; 16], u8, Option<[u8; 16]>)>,
}

impl UserDir {
    /// Resolve this user's UA: lowest-sg_rank SG that has the app visible,
    /// ties broken by device_uuid. Returns (device_uuid, app_id).
    fn resolve_ua(&self) -> Option<([u8; 16], [u8; 16])> {
        self.sgs.iter()
            .filter_map(|(d, rank, app)| app.map(|a| (*rank, *d, a)))
            .min_by(|x, y| x.0.cmp(&y.0).then(x.1.cmp(&y.1)))
            .map(|(_, d, a)| (d, a))
    }
}

/// Reverse map entry: who a `sender_app_id` resolves to, from our snapshot.
/// The foundation of auth-by-stamp (description.md, *Permissions*).
#[derive(Clone, Debug)]
struct AppOwner {
    /// Captured now; Phase 3 reads it to check a sender against a room's member
    /// list (which is keyed by user).
    #[allow(dead_code)]
    user_uuid: [u8; 16],
    device_uuid: [u8; 16],
    label: String,
    own_user: bool,
}

/// A DG client currently attached to this node (only meaningful when we are a
/// UserAgent). Populated by `CLIENT_ATTACH`.
#[derive(Clone, Debug, Serialize)]
struct AttachedClient {
    device_uuid_hex: String,
    app_id_hex: String,
    label: String,
    last_seen: u64,
}

/// This node's view of its delegation to its UA (only meaningful for a DG).
#[derive(Clone, Debug, Default, Serialize)]
struct AttachState {
    /// Last time we sent a CLIENT_ATTACH.
    last_sent: Option<u64>,
    /// Last time the UA acked.
    last_ack: Option<u64>,
}

/// A room member, identified by user (members are users, not devices).
#[derive(Clone, Debug, Serialize)]
struct Member {
    user_uuid_hex: String,
    #[serde(skip)]
    user_uuid: [u8; 16],
    alias: String,
    /// RH-side: has this member's UA announced itself (HELLO) yet?
    present: bool,
}

/// One ordered chat message. The RH assigns `seq`; everyone applies in seq
/// order (idempotent by `(room_id, seq)`).
#[derive(Clone, Debug, Serialize)]
struct ChatMsg {
    seq: u64,
    sender_user_hex: String,
    sender_alias: String,
    sender_is_self: bool,
    ts_ms: u64,
    text: String,
}

/// One chat room, as known by this node. The RH holds the authoritative copy;
/// a member's UA holds its own copy with `is_rh = false`.
#[derive(Clone, Debug, Serialize)]
struct RoomState {
    room_id_hex: String,
    #[serde(skip)]
    room_id: [u8; 16],
    name: String,
    host_user_hex: String,
    #[serde(skip)]
    host_user: [u8; 16],
    /// The Room Host address (who to send POST/HELLO/LEAVE to). For the RH
    /// itself this is its own UA address.
    rh_device_hex: String,
    #[serde(skip)]
    rh_device: [u8; 16],
    #[serde(skip)]
    rh_app_id: [u8; 16],
    /// True when this node is the RH (authority) for the room.
    is_rh: bool,
    members: Vec<Member>,
    retention_mode: u8,
    join_history_mode: u8,
    /// Member-side: timestamp we last sent HELLO to the RH (None = not yet).
    hello_sent: Option<u64>,
    /// This node's copy of the room's ordered message history.
    messages: Vec<ChatMsg>,
    /// RH-side: next `seq` to assign. Member-side: unused (always 1).
    next_seq: u64,
    /// Highest `seq` we have applied (for idempotent apply / Phase 5 cursor).
    last_seq: u64,
    /// RH-side: per-member highest acked `seq` — drives retry + reconnect replay.
    #[serde(skip)]
    member_acks: HashMap<[u8; 16], u64>,
}

#[derive(Clone, Debug, Serialize)]
struct LogEntry {
    sender: String,
    /// Decoded msg_type name (e.g. "DEV_TEXT", or "UNKNOWN(0x42)").
    kind: String,
    /// Human-readable rendering of the frame (text for DEV_TEXT, else a note).
    detail: String,
    timestamp: u64,
}

struct Inner {
    token: Option<[u8; 16]>,
    app_info: Option<AppInfo>,
    destinations: Vec<Destination>,
    /// Auth-by-stamp reverse map: sender_app_id → who it belongs to.
    app_owners: HashMap<[u8; 16], AppOwner>,
    /// Our own user's uuid (from the get-data owner field).
    own_user_uuid: Option<[u8; 16]>,
    /// This local device's uuid and this app instance's id (for HELLO/INVITE).
    local_device: Option<[u8; 16]>,
    local_app_id: Option<[u8; 16]>,
    /// This node's role for its own user.
    role: Role,
    /// The resolved User Agent for our user (delegation target for a DG).
    ua: Option<UaRef>,
    /// DGs currently attached to us (only populated when we are a UserAgent).
    attached_clients: Vec<AttachedClient>,
    /// Our delegation state toward our UA (only meaningful for a DG).
    attach: AttachState,
    /// Directory of all users we can see, for UA resolution (keyed by user uuid).
    directory: HashMap<[u8; 16], UserDir>,
    /// Rooms this node knows about, keyed by room_id.
    rooms: HashMap<[u8; 16], RoomState>,
    log: Vec<LogEntry>,
    last_fetch_ok: Option<u64>,
}

impl Inner {
    fn new(token: Option<[u8; 16]>) -> Self {
        Inner {
            token,
            app_info: None,
            destinations: Vec::new(),
            app_owners: HashMap::new(),
            own_user_uuid: None,
            local_device: None,
            local_app_id: None,
            role: Role::Unknown,
            ua: None,
            attached_clients: Vec::new(),
            attach: AttachState::default(),
            directory: HashMap::new(),
            rooms: HashMap::new(),
            log: Vec::new(),
            last_fetch_ok: None,
        }
    }
}

struct AppState {
    /// Receives op 0x04 pushes from pnet. Background loop owns this.
    push_socket: Arc<UdpSocket>,
    /// Sends registration/get-data/send requests; receives their replies.
    ctrl_socket: Arc<UdpSocket>,
    pnet_addr: SocketAddr,
    inner: Mutex<Inner>,
}

// ── Binary helpers ────────────────────────────────────────────────────────────

fn push_str(buf: &mut Vec<u8>, s: &str) {
    buf.push(s.len() as u8);
    buf.extend_from_slice(s.as_bytes());
}

fn read_str(data: &[u8], pos: &mut usize) -> Option<String> {
    let len = *data.get(*pos)? as usize;
    *pos += 1;
    let s = std::str::from_utf8(data.get(*pos..*pos + len)?).ok()?.to_string();
    *pos += len;
    Some(s)
}

fn read_bytes<const N: usize>(data: &[u8], pos: &mut usize) -> Option<[u8; N]> {
    let arr: [u8; N] = data.get(*pos..*pos + N)?.try_into().ok()?;
    *pos += N;
    Some(arr)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Parse 32 lowercase hex chars into a 16-byte uuid.
fn hex_to_uuid(s: &str) -> Option<[u8; 16]> {
    if s.len() != 32 { return None; }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(s.get(i * 2..i * 2 + 2)?, 16).ok()?;
    }
    Some(out)
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Generate a 16-byte room id. Reads OS entropy (`/dev/urandom`); falls back to
/// time + a process-local counter if that is unavailable. Only the RH mints
/// these, so uniqueness within one RH is all that matters.
fn gen_uuid() -> [u8; 16] {
    use std::io::Read;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut out = [0u8; 16];
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        if f.read_exact(&mut out).is_ok() {
            return out;
        }
    }
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;
    let ctr = COUNTER.fetch_add(1, Ordering::Relaxed);
    out[..8].copy_from_slice(&nanos.to_be_bytes());
    out[8..].copy_from_slice(&ctr.to_be_bytes());
    out
}

// ── pnet protocol ─────────────────────────────────────────────────────────────

fn build_register(push_port: u16) -> Vec<u8> {
    let mut buf = vec![OP_REGISTER];
    push_str(&mut buf, APP_ALIAS);
    buf.extend_from_slice(&push_port.to_be_bytes()); // push delivery port
    push_str(&mut buf, APP_PROTOCOL);
    buf
}

fn build_get_data(token: &[u8; 16]) -> Vec<u8> {
    let mut buf = vec![OP_GET_DATA];
    buf.extend_from_slice(token);
    buf
}

fn build_send(token: &[u8; 16], dest_device_uuid: &[u8], dest_app_id: &[u8; 16], payload: &[u8]) -> Vec<u8> {
    let mut buf = vec![OP_SEND];
    buf.extend_from_slice(token);
    buf.extend_from_slice(dest_device_uuid);
    buf.extend_from_slice(dest_app_id);
    buf.extend_from_slice(payload);
    buf
}

/// One of our own user's SG devices, for role/UA resolution.
struct OwnSg {
    device_uuid: [u8; 16],
    label: String,
    sg_rank: u8,
    /// The `pnet-chat` app on this SG, if visible in our snapshot yet.
    chat_app_id: Option<[u8; 16]>,
}

/// Parse op 0x02 response and update shared state. Layout mirrors the proven
/// pnet_deliverer parser, extended to (a) build the auth-by-stamp reverse map
/// (`app_id → owner`), and (b) resolve this node's role and its User Agent —
/// its own user's top-ranked SG (description.md, *Roles and topology*).
fn parse_get_data(reply: &[u8], inner: &mut Inner) {
    let mut pos = 1usize; // skip OK byte

    // App's own data.
    let Some(app_id) = read_bytes::<16>(reply, &mut pos) else { return };
    let Some(alias) = read_str(reply, &mut pos) else { return };
    pos += 6; // ip(4) + port(2)
    let Some(&approved_byte) = reply.get(pos) else { return };
    pos += 1;
    pos += 16; // token
    let Some(local_device_uuid) = read_bytes::<16>(reply, &mut pos) else { return };

    inner.app_info = Some(AppInfo {
        id_hex: hex(&app_id),
        alias,
        approved: approved_byte != 0,
    });

    // Owner alias + uuid.
    let Some(owner_alias) = read_str(reply, &mut pos) else { return };
    let Some(owner_uuid) = read_bytes::<16>(reply, &mut pos) else { return };

    let mut destinations: Vec<Destination> = Vec::new();
    let mut app_owners: HashMap<[u8; 16], AppOwner> = HashMap::new();
    let mut directory: HashMap<[u8; 16], UserDir> = HashMap::new();

    // Own-user topology, for role + UA resolution.
    let mut own_sgs: Vec<OwnSg> = Vec::new();
    let mut local_grade: Option<u8> = None;
    let mut local_sg_rank: u8 = 0;

    // Own devices.
    let Some(&device_count) = reply.get(pos) else { return };
    pos += 1;
    for _ in 0..device_count {
        let Some(dev_uuid) = read_bytes::<16>(reply, &mut pos) else { return };
        let Some(dev_alias) = read_str(reply, &mut pos) else { return };
        let Some(&grade_byte) = reply.get(pos) else { return };
        let Some(&sg_rank) = reply.get(pos + 1) else { return };
        pos += 2; // grade + sg_rank
        let Some(&host_count) = reply.get(pos) else { return };
        pos += 1;
        for _ in 0..host_count {
            if read_str(reply, &mut pos).is_none() { return }
        }
        let Some(&app_count) = reply.get(pos) else { return };
        pos += 1;
        let is_local_device = dev_uuid == local_device_uuid;
        if is_local_device {
            local_grade = Some(grade_byte);
            local_sg_rank = sg_rank;
        }
        let mut dev_chat_app: Option<[u8; 16]> = None;
        for _ in 0..app_count {
            let Some(aid) = read_bytes::<16>(reply, &mut pos) else { return };
            let Some(app_alias) = read_str(reply, &mut pos) else { return };
            pos += 4 + 2 + 1; // ip + port + user_approved
            let label = format!("{dev_alias} / {app_alias}");
            app_owners.insert(aid, AppOwner {
                user_uuid: owner_uuid,
                device_uuid: dev_uuid,
                label: label.clone(),
                own_user: true,
            });
            if app_alias == APP_ALIAS && dev_chat_app.is_none() {
                dev_chat_app = Some(aid);
            }
            // Exclude only the specific app instance that is this very client
            // (same device AND same app uuid).
            if !(is_local_device && aid == app_id) {
                destinations.push(Destination {
                    label,
                    user_uuid_hex: hex(&owner_uuid),
                    device_uuid: dev_uuid.to_vec(),
                    grade: if grade_byte == GRADE_SG { "SG".into() } else { "DG".into() },
                    sg_rank,
                    own_user: true,
                    app_id_hex: hex(&aid),
                    app_id: aid,
                });
            }
        }
        if grade_byte == GRADE_SG {
            own_sgs.push(OwnSg {
                device_uuid: dev_uuid,
                label: dev_alias,
                sg_rank,
                chat_app_id: dev_chat_app,
            });
        }
    }

    // Own user directory entry (SG devices only — UA resolution needs SGs).
    directory.insert(owner_uuid, UserDir {
        alias: owner_alias,
        own: true,
        sgs: own_sgs.iter().map(|s| (s.device_uuid, s.sg_rank, s.chat_app_id)).collect(),
    });

    // Contacts.
    let Some(&contact_count) = reply.get(pos) else { return };
    pos += 1;
    for _ in 0..contact_count {
        let Some(contact_alias) = read_str(reply, &mut pos) else { return };
        let Some(contact_uuid) = read_bytes::<16>(reply, &mut pos) else { return };
        let Some(&device_count) = reply.get(pos) else { return };
        pos += 1;
        let mut contact_sgs: Vec<([u8; 16], u8, Option<[u8; 16]>)> = Vec::new();
        for _ in 0..device_count {
            let Some(dev_uuid) = read_bytes::<16>(reply, &mut pos) else { return };
            let Some(dev_alias) = read_str(reply, &mut pos) else { return };
            let Some(&grade_byte) = reply.get(pos) else { return };
            let Some(&sg_rank) = reply.get(pos + 1) else { return };
            pos += 2; // grade + sg_rank
            let Some(&host_count) = reply.get(pos) else { return };
            pos += 1;
            for _ in 0..host_count {
                if read_str(reply, &mut pos).is_none() { return }
            }
            let Some(&app_count) = reply.get(pos) else { return };
            pos += 1;
            let mut dev_chat_app: Option<[u8; 16]> = None;
            for _ in 0..app_count {
                // Contact apps: only approved ones are listed, no ip/port.
                let Some(aid) = read_bytes::<16>(reply, &mut pos) else { return };
                let Some(app_alias) = read_str(reply, &mut pos) else { return };
                let label = format!("{contact_alias} / {dev_alias} / {app_alias}");
                app_owners.insert(aid, AppOwner {
                    user_uuid: contact_uuid,
                    device_uuid: dev_uuid,
                    label: label.clone(),
                    own_user: false,
                });
                if app_alias == APP_ALIAS && dev_chat_app.is_none() {
                    dev_chat_app = Some(aid);
                }
                destinations.push(Destination {
                    label,
                    user_uuid_hex: hex(&contact_uuid),
                    device_uuid: dev_uuid.to_vec(),
                    grade: if grade_byte == GRADE_SG { "SG".into() } else { "DG".into() },
                    sg_rank,
                    own_user: false,
                    app_id_hex: hex(&aid),
                    app_id: aid,
                });
            }
            if grade_byte == GRADE_SG {
                contact_sgs.push((dev_uuid, sg_rank, dev_chat_app));
            }
        }
        directory.insert(contact_uuid, UserDir {
            alias: contact_alias,
            own: false,
            sgs: contact_sgs,
        });
    }

    // ── Resolve role + User Agent ─────────────────────────────────────────────
    // The UA is our user's top-ranked SG (lowest sg_rank). Ties break on the
    // lowest device_uuid so every node picks the same one deterministically.
    let top_sg = own_sgs.iter()
        .min_by(|a, b| a.sg_rank.cmp(&b.sg_rank).then(a.device_uuid.cmp(&b.device_uuid)));

    let (role, ua) = match (local_grade, top_sg) {
        (Some(GRADE_SG), Some(top)) if top.device_uuid == local_device_uuid => {
            // We are the top-ranked SG — the acting UA.
            (Role::UserAgent, Some(UaRef {
                device_uuid_hex: hex(&local_device_uuid),
                device_uuid: local_device_uuid,
                label: top.label.clone(),
                sg_rank: local_sg_rank,
                app_id_hex: Some(hex(&app_id)),
                app_id: Some(app_id),
                is_self: true,
            }))
        }
        (Some(GRADE_SG), Some(top)) => {
            // An SG, but not the top one — a standby/mirror. UA is the top SG.
            (Role::SgStandby, Some(ua_ref_from(top, false)))
        }
        (Some(_), Some(top)) => {
            // A DG — delegates to the top SG (its UA).
            (Role::DataGuest, Some(ua_ref_from(top, false)))
        }
        (Some(_), None) => {
            // A DG-only user with no SG: no UA exists (description.md,
            // *Members without an SG*).
            (Role::DataGuest, None)
        }
        (None, _) => (Role::Unknown, None),
    };

    inner.destinations = destinations;
    inner.app_owners = app_owners;
    inner.directory = directory;
    inner.own_user_uuid = Some(owner_uuid);
    inner.local_device = Some(local_device_uuid);
    inner.local_app_id = Some(app_id);
    inner.role = role;
    inner.ua = ua;
}

fn ua_ref_from(sg: &OwnSg, is_self: bool) -> UaRef {
    UaRef {
        device_uuid_hex: hex(&sg.device_uuid),
        device_uuid: sg.device_uuid,
        label: sg.label.clone(),
        sg_rank: sg.sg_rank,
        app_id_hex: sg.chat_app_id.map(|a| hex(&a)),
        app_id: sg.chat_app_id,
        is_self,
    }
}

// ── Token persistence ─────────────────────────────────────────────────────────

fn load_token(path: &str) -> Option<[u8; 16]> {
    let bytes = std::fs::read(path).ok()?;
    bytes.as_slice().try_into().ok()
}

fn save_token(path: &str, token: &[u8; 16]) {
    if let Err(e) = std::fs::write(path, token) {
        eprintln!("[token] failed to save: {e}");
    }
}

// ── Registration ──────────────────────────────────────────────────────────────

async fn register(ctrl: &UdpSocket, pnet_addr: SocketAddr, push_port: u16) -> Option<[u8; 16]> {
    ctrl.send_to(&build_register(push_port), pnet_addr).await.ok()?;

    let mut buf = [0u8; 512];
    let (len, _) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctrl.recv_from(&mut buf),
    )
    .await
    .ok()?
    .ok()?;

    let reply = &buf[..len];
    if reply.len() < 17 || reply[0] != STATUS_OK {
        eprintln!("[register] failed: {:?}", &reply[..reply.len().min(4)]);
        return None;
    }
    Some(reply[1..17].try_into().unwrap())
}

async fn fetch_data(ctrl: &UdpSocket, pnet_addr: SocketAddr, token: &[u8; 16], inner: &Mutex<Inner>) {
    if ctrl.send_to(&build_get_data(token), pnet_addr).await.is_err() {
        return;
    }

    let mut buf = vec![0u8; 4096];
    let Ok(Ok((len, _))) = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctrl.recv_from(&mut buf),
    )
    .await
    else {
        eprintln!("[fetch_data] timeout or error");
        return;
    };

    let reply = &buf[..len];
    if reply.is_empty() || reply[0] != STATUS_OK {
        eprintln!("[fetch_data] bad reply: {:?}", &reply[..reply.len().min(4)]);
        return;
    }

    let mut guard = inner.lock().unwrap();
    parse_get_data(reply, &mut guard);
    if guard.app_info.is_some() {
        guard.last_fetch_ok = Some(now_secs());
    }
}

/// Send one enveloped app frame to `(dest_device, dest_app_id)` via pnet.
async fn send_frame(
    ctrl: &UdpSocket,
    pnet_addr: SocketAddr,
    token: &[u8; 16],
    dest_device: &[u8; 16],
    dest_app_id: &[u8; 16],
    msg_type: u8,
    room_id: &[u8; ROOM_ID_LEN],
    body: &[u8],
) -> bool {
    let payload = encode_envelope(msg_type, room_id, body);
    let pkt = build_send(token, dest_device, dest_app_id, &payload);
    ctrl.send_to(&pkt, pnet_addr).await.is_ok()
}

// ── UDP push receive loop ─────────────────────────────────────────────────────

async fn push_receive_loop(state: Arc<AppState>) {
    let mut buf = vec![0u8; 4096];
    loop {
        let (len, _) = match state.push_socket.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => { eprintln!("[push_recv] {e}"); continue; }
        };

        let data = &buf[..len];
        // Push format: [0x04][sender_app_id: 16][payload].
        if data.len() < 17 || data[0] != OP_PUSH {
            continue;
        }
        let sender_id: [u8; 16] = data[1..17].try_into().unwrap();
        let payload = &data[17..];

        // Process under the lock; collect any outbound frames to send after
        // releasing it (never hold the std Mutex across an await).
        let mut outs: Vec<Outbound> = Vec::new();
        let token = {
            let mut inner = state.inner.lock().unwrap();

            // Auth-by-stamp: resolve who this packet is really from.
            let owner = inner.app_owners.get(&sender_id).cloned();
            let sender = owner.as_ref().map(|o| o.label.clone())
                .unwrap_or_else(|| format!("app#{} (unresolved)", hex(&sender_id)));

            let (kind, detail) = match decode_envelope(payload) {
                Some(env) => {
                    let kind = if msg::name(env.msg_type) == "UNKNOWN" {
                        format!("UNKNOWN(0x{:02x})", env.msg_type)
                    } else {
                        msg::name(env.msg_type).to_string()
                    };
                    let detail = handle_frame(&mut inner, &env, sender_id, owner.as_ref(), &mut outs);
                    (kind, detail)
                }
                None => (
                    "BAD_FRAME".to_string(),
                    format!("undecodable payload ({} byte(s))", payload.len()),
                ),
            };

            eprintln!("[recv] from {sender}: {kind} — {detail}");
            inner.log.push(LogEntry { sender, kind, detail, timestamp: now_secs() });
            inner.token
        };

        if let Some(token) = token {
            flush(&state, &token, outs).await;
        }
    }
}

/// Send each queued outbound frame via pnet.
async fn flush(state: &Arc<AppState>, token: &[u8; 16], outs: Vec<Outbound>) {
    for o in outs {
        send_frame(&state.ctrl_socket, state.pnet_addr, token,
                   &o.dest_device, &o.dest_app, o.msg_type, &o.room_id, &o.body).await;
    }
}

/// Apply one decoded inbound frame to state, returning a human-readable detail
/// for the log and queuing any outbound frames into `outs`.
fn handle_frame(
    inner: &mut Inner,
    env: &Envelope<'_>,
    sender_id: [u8; 16],
    owner: Option<&AppOwner>,
    outs: &mut Vec<Outbound>,
) -> String {
    match env.msg_type {
        msg::DEV_TEXT => decode_dev_text(env.body)
            .unwrap_or_else(|| "<malformed DEV_TEXT>".into()),

        msg::CLIENT_ATTACH => {
            let Some(owner) = owner else {
                return "attach from unresolved sender — dropped (waiting on sync)".into();
            };
            if inner.role != Role::UserAgent {
                return format!("attach from {} ignored — we are not a UserAgent", owner.label);
            }
            if !owner.own_user {
                return format!("attach from {} ignored — not our user", owner.label);
            }
            let now = now_secs();
            if let Some(c) = inner.attached_clients.iter_mut()
                .find(|c| c.app_id_hex == hex(&sender_id))
            {
                c.last_seen = now;
            } else {
                inner.attached_clients.push(AttachedClient {
                    device_uuid_hex: hex(&owner.device_uuid),
                    app_id_hex: hex(&sender_id),
                    label: owner.label.clone(),
                    last_seen: now,
                });
            }
            outs.push(Outbound {
                dest_device: owner.device_uuid, dest_app: sender_id,
                msg_type: msg::CLIENT_ATTACH_ACK, room_id: ZERO_ROOM, body: Vec::new(),
            });
            format!("attach from {} — acked", owner.label)
        }

        msg::CLIENT_ATTACH_ACK => {
            if inner.role == Role::DataGuest {
                inner.attach.last_ack = Some(now_secs());
                "UA acknowledged our attach".into()
            } else {
                "attach-ack received but we are not a DG".into()
            }
        }

        msg::OPEN_ROOM => handle_open_room(inner, env, sender_id, owner, outs),
        msg::INVITE => handle_invite(inner, env, owner, outs),
        msg::HELLO => handle_hello(inner, env, owner, outs),
        msg::ADD_MEMBER => handle_add_member(inner, env, owner, outs),
        msg::REMOVE_MEMBER => handle_remove_member(inner, env, owner, outs),
        msg::LEAVE => handle_leave(inner, env, owner, outs),
        msg::MEMBER_UPDATE => handle_member_update(inner, env, owner),
        msg::POST => handle_post(inner, env, owner, outs),
        msg::MSG => handle_msg(inner, env, owner, outs),
        msg::ACK => handle_ack(inner, env, owner),
        msg::HISTORY_REQ => handle_history_req(inner, env, owner, outs),
        msg::HISTORY_RESP => handle_history_resp(inner, env, owner, outs),

        _ => format!(
            "room {}…, {} body byte(s) — not handled until a later phase",
            &hex(&env.room_id)[..8],
            env.body.len()
        ),
    }
}

// ── Room lifecycle (RH + member side) ─────────────────────────────────────────

/// Build the member list for a room from invited user uuids, looking up aliases
/// in the directory. Skips the host's own user and unknown/self entries.
fn build_members(inner: &Inner, member_uuids: &[[u8; 16]]) -> Vec<Member> {
    let own = inner.own_user_uuid;
    member_uuids.iter().filter(|u| Some(**u) != own).map(|u| {
        let alias = inner.directory.get(u).map(|d| d.alias.clone())
            .unwrap_or_else(|| format!("user#{}", &hex(u)[..8]));
        Member { user_uuid_hex: hex(u), user_uuid: *u, alias, present: false }
    }).collect()
}

/// Queue an INVITE to a member's User Agent, if it can be resolved right now.
/// Returns false if the member's UA isn't resolvable yet (caller may retry).
fn queue_invite(inner: &Inner, room: &RoomState, member: &Member, outs: &mut Vec<Outbound>) -> bool {
    let Some(dir) = inner.directory.get(&member.user_uuid) else { return false };
    let Some((dev, app)) = dir.resolve_ua() else { return false };
    let body = encode_invite(&room.name, &room.host_user, &room.rh_device, &room.rh_app_id,
                             room.retention_mode, room.join_history_mode, &room.members);
    outs.push(Outbound { dest_device: dev, dest_app: app, msg_type: msg::INVITE,
                         room_id: room.room_id, body });
    true
}

/// Fan a MEMBER_UPDATE out to every current member's UA.
fn fan_member_update(inner: &Inner, room: &RoomState, change: u8, user: &[u8; 16], alias: &str, outs: &mut Vec<Outbound>) {
    let body = encode_member_update(change, user, alias);
    for m in &room.members {
        if let Some((dev, app)) = inner.directory.get(&m.user_uuid).and_then(|d| d.resolve_ua()) {
            outs.push(Outbound { dest_device: dev, dest_app: app, msg_type: msg::MEMBER_UPDATE,
                                 room_id: room.room_id, body: body.clone() });
        }
    }
}

/// Create a room with this node as the RH (we are the host's UA). Mints the
/// room id, stores the authoritative state, sends INVITEs, and (when an
/// originating DG is given) a ROOM_CREATED back. Returns the new room id.
fn create_room(
    inner: &mut Inner, name: &str, retention: u8, join_hist: u8,
    member_uuids: &[[u8; 16]], originator: Option<([u8; 16], [u8; 16])>,
    outs: &mut Vec<Outbound>,
) -> Option<[u8; 16]> {
    let (host_user, rh_device, rh_app) = match (inner.own_user_uuid, inner.local_device, inner.local_app_id) {
        (Some(u), Some(d), Some(a)) => (u, d, a),
        _ => return None,
    };
    let room_id = gen_uuid();
    let members = build_members(inner, member_uuids);
    let room = RoomState {
        room_id_hex: hex(&room_id), room_id,
        name: name.to_string(),
        host_user_hex: hex(&host_user), host_user,
        rh_device_hex: hex(&rh_device), rh_device, rh_app_id: rh_app,
        is_rh: true,
        members,
        retention_mode: retention, join_history_mode: join_hist,
        hello_sent: None,
        messages: Vec::new(),
        next_seq: 1,
        last_seq: 0,
        member_acks: HashMap::new(),
    };
    for m in &room.members {
        queue_invite(inner, &room, m, outs);
    }
    if let Some((dev, app)) = originator {
        outs.push(Outbound { dest_device: dev, dest_app: app, msg_type: msg::ROOM_CREATED,
                             room_id, body: Vec::new() });
    }
    inner.rooms.insert(room_id, room);
    Some(room_id)
}

fn handle_open_room(inner: &mut Inner, env: &Envelope<'_>, sender_id: [u8; 16], owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    // Control message: only from one of the host's own devices (auth-by-stamp).
    let Some(owner) = owner else { return "OPEN_ROOM from unresolved sender — dropped".into() };
    if inner.role != Role::UserAgent {
        return "OPEN_ROOM ignored — we are not a UserAgent (cannot be an RH)".into();
    }
    if !owner.own_user {
        return format!("OPEN_ROOM from {} rejected — not our user", owner.label);
    }
    let Some(open) = decode_open_room(env.body) else { return "OPEN_ROOM malformed".into() };
    // ROOM_CREATED goes back to the originating device + app (the stamp).
    let originator = (owner.device_uuid, sender_id);
    match create_room(inner, &open.name, open.retention, open.join_hist, &open.members,
                       Some(originator), outs) {
        Some(rid) => format!("created room '{}' ({}…) with {} member(s)", open.name, &hex(&rid)[..8], open.members.len()),
        None => "OPEN_ROOM failed — local identity not resolved yet".into(),
    }
}

fn handle_invite(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "INVITE from unresolved sender — dropped".into() };
    let Some(inv) = decode_invite(env.body) else { return "INVITE malformed".into() };
    // We must be able to act as this user's UA to hold the room and HELLO back.
    let (my_device, my_app) = match (inner.local_device, inner.local_app_id) {
        (Some(d), Some(a)) => (d, a),
        _ => return "INVITE received but local identity not resolved yet".into(),
    };
    let members: Vec<Member> = inv.members.iter().map(|(u, a)| Member {
        user_uuid_hex: hex(u), user_uuid: *u, alias: a.clone(), present: false,
    }).collect();

    // Idempotent: a re-INVITE (RH maintenance loop re-sending until we HELLO)
    // must not wipe history we already hold — just refresh the member list and
    // re-HELLO with our current cursor.
    let existing = inner.rooms.get(&env.room_id);
    let last_seq = existing.map(|r| r.last_seq).unwrap_or(0);
    let messages = existing.map(|r| r.messages.clone()).unwrap_or_default();
    let was_member = existing.is_some();
    let room = RoomState {
        room_id_hex: hex(&env.room_id), room_id: env.room_id,
        name: inv.name.clone(),
        host_user_hex: hex(&inv.host_user), host_user: inv.host_user,
        rh_device_hex: hex(&inv.rh_device), rh_device: inv.rh_device, rh_app_id: inv.rh_app,
        is_rh: false,
        members,
        retention_mode: inv.retention, join_history_mode: inv.join_hist,
        hello_sent: Some(now_secs()),
        messages,
        next_seq: 1,
        last_seq,
        member_acks: HashMap::new(),
    };
    // HELLO back to the RH announcing our UA address + (room, last applied seq).
    let body = encode_hello(&my_device, &my_app, &[(env.room_id, last_seq)]);
    outs.push(Outbound { dest_device: inv.rh_device, dest_app: inv.rh_app,
                         msg_type: msg::HELLO, room_id: ZERO_ROOM, body });
    inner.rooms.insert(env.room_id, room);
    if was_member {
        format!("re-INVITE to '{}' — re-HELLO sent (cursor {last_seq})", inv.name)
    } else {
        format!("invited to '{}' by {} — joined, HELLO sent", inv.name, owner.label)
    }
}

fn handle_hello(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "HELLO from unresolved sender — dropped".into() };
    let Some((dev, app, rooms)) = decode_hello(env.body) else { return "HELLO malformed".into() };
    let mut touched = 0;
    for (rid, reported_seq) in &rooms {
        let is_member = inner.rooms.get(rid)
            .map(|r| r.is_rh && r.members.iter().any(|m| m.user_uuid == owner.user_uuid))
            .unwrap_or(false);
        if !is_member { continue; }
        // Auth-by-stamp: the sender's user is a current member. Mark present and
        // seed the reconnect cursor from the reported seq, then replay the gap.
        {
            let room = inner.rooms.get_mut(rid).unwrap();
            if let Some(m) = room.members.iter_mut().find(|m| m.user_uuid == owner.user_uuid) {
                m.present = true;
            }
            let cur = room.member_acks.entry(owner.user_uuid).or_insert(0);
            if *reported_seq > *cur { *cur = *reported_seq; }
        }
        replay_to_member(inner, rid, owner.user_uuid, &dev, &app, outs);
        touched += 1;
    }
    if touched > 0 {
        format!("HELLO from {} — present + replayed in {} room(s)", owner.label, touched)
    } else {
        format!("HELLO from {} — no matching room we host", owner.label)
    }
}

fn handle_add_member(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "ADD_MEMBER from unresolved sender — dropped".into() };
    let mut pos = 0;
    let Some(target) = read_bytes::<16>(env.body, &mut pos) else { return "ADD_MEMBER malformed".into() };
    // Snapshot the room (RH-only, host-only) without holding a mutable borrow
    // across directory lookups.
    let Some(room) = inner.rooms.get(&env.room_id).cloned() else { return "ADD_MEMBER for unknown room".into() };
    if !room.is_rh { return "ADD_MEMBER ignored — we are not this room's RH".into(); }
    if !owner.own_user { return "ADD_MEMBER rejected — sender is not the host".into(); }
    // Target must be one of the host's contacts (in the directory, not our user).
    let Some(dir) = inner.directory.get(&target) else {
        return "ADD_MEMBER target not a known contact".into();
    };
    if dir.own { return "ADD_MEMBER target is our own user".into(); }
    if room.members.iter().any(|m| m.user_uuid == target) {
        return "ADD_MEMBER: already a member".into();
    }
    let alias = dir.alias.clone();
    let new_member = Member { user_uuid_hex: hex(&target), user_uuid: target, alias: alias.clone(), present: false };
    // Notify existing members, invite the new one.
    fan_member_update(inner, &room, MEMBER_CHANGE_ADD, &target, &alias, outs);
    {
        let room_mut = inner.rooms.get_mut(&env.room_id).unwrap();
        room_mut.members.push(new_member.clone());
    }
    let room_after = inner.rooms.get(&env.room_id).cloned().unwrap();
    queue_invite(inner, &room_after, &new_member, outs);
    format!("added member {alias} to '{}'", room.name)
}

fn handle_remove_member(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "REMOVE_MEMBER from unresolved sender — dropped".into() };
    let mut pos = 0;
    let Some(target) = read_bytes::<16>(env.body, &mut pos) else { return "REMOVE_MEMBER malformed".into() };
    let Some(room) = inner.rooms.get(&env.room_id).cloned() else { return "REMOVE_MEMBER for unknown room".into() };
    if !room.is_rh { return "REMOVE_MEMBER ignored — we are not this room's RH".into(); }
    if !owner.own_user { return "REMOVE_MEMBER rejected — sender is not the host".into(); }
    let Some(removed) = room.members.iter().find(|m| m.user_uuid == target).cloned() else {
        return "REMOVE_MEMBER: not a member".into();
    };
    // Notify everyone (including the removed member) before dropping them.
    fan_member_update(inner, &room, MEMBER_CHANGE_REMOVE, &target, &removed.alias, outs);
    inner.rooms.get_mut(&env.room_id).unwrap().members.retain(|m| m.user_uuid != target);
    format!("removed member {} from '{}'", removed.alias, room.name)
}

fn handle_leave(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "LEAVE from unresolved sender — dropped".into() };
    let Some(room) = inner.rooms.get(&env.room_id).cloned() else { return "LEAVE for unknown room".into() };
    if !room.is_rh { return "LEAVE ignored — we are not this room's RH".into(); }
    let Some(member) = room.members.iter().find(|m| m.user_uuid == owner.user_uuid).cloned() else {
        return "LEAVE from a non-member — ignored".into();
    };
    inner.rooms.get_mut(&env.room_id).unwrap().members.retain(|m| m.user_uuid != owner.user_uuid);
    let room_after = inner.rooms.get(&env.room_id).cloned().unwrap();
    fan_member_update(inner, &room_after, MEMBER_CHANGE_LEAVE, &member.user_uuid, &member.alias, outs);
    format!("{} left '{}'", member.alias, room.name)
}

fn handle_member_update(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>) -> String {
    let Some(owner) = owner else { return "MEMBER_UPDATE from unresolved sender — dropped".into() };
    let Some((change, user, alias)) = decode_member_update(env.body) else { return "MEMBER_UPDATE malformed".into() };
    let own_user = inner.own_user_uuid;
    let Some(room) = inner.rooms.get_mut(&env.room_id) else { return "MEMBER_UPDATE for unknown room".into() };
    // Only accept from the room's RH device.
    if owner.device_uuid != room.rh_device {
        return "MEMBER_UPDATE not from our RH — dropped".into();
    }
    // If we are the one being removed/leaving, drop the whole room.
    if (change == MEMBER_CHANGE_REMOVE) && Some(user) == own_user {
        let name = room.name.clone();
        inner.rooms.remove(&env.room_id);
        return format!("we were removed from '{name}' — left the room");
    }
    match change {
        MEMBER_CHANGE_ADD => {
            if !room.members.iter().any(|m| m.user_uuid == user) {
                room.members.push(Member { user_uuid_hex: hex(&user), user_uuid: user, alias: alias.clone(), present: false });
            }
            format!("member added to '{}': {alias}", room.name)
        }
        MEMBER_CHANGE_REMOVE | MEMBER_CHANGE_LEAVE => {
            room.members.retain(|m| m.user_uuid != user);
            let verb = if change == MEMBER_CHANGE_LEAVE { "left" } else { "removed from" };
            format!("{alias} {verb} '{}'", room.name)
        }
        _ => "MEMBER_UPDATE unknown change kind".into(),
    }
}

// ── Text messaging (Phase 4): POST → RH seq → MSG fan-out ─────────────────────

/// Resolve a user's display alias from the room's member list, then the
/// directory; mark whether it is us.
fn resolve_sender(inner: &Inner, room_id: &[u8; 16], user: &[u8; 16]) -> (String, bool) {
    let is_self = Some(*user) == inner.own_user_uuid;
    let alias = inner.rooms.get(room_id)
        .and_then(|r| r.members.iter().find(|m| m.user_uuid == *user).map(|m| m.alias.clone()))
        .or_else(|| inner.directory.get(user).map(|d| d.alias.clone()))
        .unwrap_or_else(|| if is_self { "me".into() } else { format!("user#{}", &hex(user)[..8]) });
    (alias, is_self)
}

/// Append an ordered message to a room's local history, idempotent by `seq`.
/// Returns true if it was newly applied.
fn apply_message(inner: &mut Inner, room_id: &[u8; 16], seq: u64, sender_user: [u8; 16], ts_ms: u64, text: String) -> bool {
    let (alias, is_self) = resolve_sender(inner, room_id, &sender_user);
    let Some(room) = inner.rooms.get_mut(room_id) else { return false };
    if room.messages.iter().any(|m| m.seq == seq) { return false; }
    room.messages.push(ChatMsg {
        seq, sender_user_hex: hex(&sender_user),
        sender_alias: alias, sender_is_self: is_self, ts_ms, text,
    });
    room.messages.sort_by_key(|m| m.seq);
    if seq > room.last_seq { room.last_seq = seq; }
    true
}

/// RH-side acceptance of a new message: assign the next `seq`, append to our
/// authoritative history, and fan a `MSG` out to every member's UA. Returns the
/// assigned seq.
fn rh_accept_message(inner: &mut Inner, room_id: &[u8; 16], sender_user: [u8; 16], cid: [u8; 16], text: String, outs: &mut Vec<Outbound>) -> Result<u64, String> {
    let (seq, members) = {
        let room = inner.rooms.get(room_id).ok_or("unknown room")?;
        if !room.is_rh { return Err("not this room's RH".into()); }
        (room.next_seq, room.members.clone())
    };
    let ts = now_ms();
    inner.rooms.get_mut(room_id).unwrap().next_seq = seq + 1;
    apply_message(inner, room_id, seq, sender_user, ts, text.clone());
    let body = encode_msg(seq, &sender_user, ts, &cid, &text);
    for m in &members {
        if let Some((dev, app)) = inner.directory.get(&m.user_uuid).and_then(|d| d.resolve_ua()) {
            outs.push(Outbound { dest_device: dev, dest_app: app, msg_type: msg::MSG,
                                 room_id: *room_id, body: body.clone() });
        }
    }
    Ok(seq)
}

fn handle_post(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "POST from unresolved sender — dropped".into() };
    let Some((cid, text)) = decode_post(env.body) else { return "POST malformed".into() };
    // Auth-by-stamp: only a current member (or the host) may post.
    let (is_rh, allowed) = match inner.rooms.get(&env.room_id) {
        Some(r) => (r.is_rh, r.members.iter().any(|m| m.user_uuid == owner.user_uuid) || owner.own_user),
        None => return "POST for unknown room".into(),
    };
    if !is_rh { return "POST ignored — we are not this room's RH".into(); }
    if !allowed { return format!("POST from non-member {} dropped", owner.label); }
    match rh_accept_message(inner, &env.room_id, owner.user_uuid, cid, text, outs) {
        Ok(seq) => format!("POST from {} ordered as seq {seq}", owner.label),
        Err(e) => e,
    }
}

/// The highest seq N such that every seq in 1..=N is present — the cumulative
/// applied-through cursor we ACK and report in HELLO (gaps below it block it).
fn contiguous_seq(room: &RoomState) -> u64 {
    let mut n = 0u64;
    loop {
        if room.messages.iter().any(|m| m.seq == n + 1) { n += 1; } else { return n; }
    }
}

fn uuid_from_hex(s: &str) -> [u8; 16] { hex_to_uuid(s).unwrap_or([0u8; 16]) }

/// Address of a room's RH (for member → RH frames). None if we don't hold the room.
fn rh_addr(inner: &Inner, room_id: &[u8; 16]) -> Option<([u8; 16], [u8; 16])> {
    inner.rooms.get(room_id).filter(|r| !r.is_rh).map(|r| (r.rh_device, r.rh_app_id))
}

/// Queue a frame from a member to its room's RH.
fn to_rh(inner: &Inner, room_id: &[u8; 16], msg_type: u8, body: Vec<u8>, outs: &mut Vec<Outbound>) {
    if let Some((dev, app)) = rh_addr(inner, room_id) {
        outs.push(Outbound { dest_device: dev, dest_app: app, msg_type, room_id: *room_id, body });
    }
}

/// Send an ACK for our current contiguous cursor to a room's RH.
fn ack_to_rh(inner: &Inner, room_id: &[u8; 16], outs: &mut Vec<Outbound>) {
    if let Some(room) = inner.rooms.get(room_id) {
        if room.is_rh { return; } // the RH does not ACK itself
        let cursor = contiguous_seq(room);
        to_rh(inner, room_id, msg::ACK, encode_ack(cursor), outs);
    }
}

fn handle_msg(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "MSG from unresolved sender — dropped".into() };
    let Some(m) = decode_msg(env.body) else { return "MSG malformed".into() };
    // Accept ordered messages only from the room's current RH.
    let from_rh = inner.rooms.get(&env.room_id).map(|r| r.rh_device == owner.device_uuid).unwrap_or(false);
    if !from_rh { return "MSG not from our RH — dropped".into(); }
    let prev_cursor = inner.rooms.get(&env.room_id).map(contiguous_seq).unwrap_or(0);
    let applied = apply_message(inner, &env.room_id, m.seq, m.sender_user, m.ts_ms, m.text);
    // If this MSG jumped ahead of our contiguous cursor, ask the RH for the gap.
    if m.seq > prev_cursor + 1 {
        to_rh(inner, &env.room_id, msg::HISTORY_REQ, encode_history_req(prev_cursor, 0), outs);
    }
    // ACK our (possibly advanced) contiguous cursor.
    ack_to_rh(inner, &env.room_id, outs);
    if applied { format!("MSG seq {} applied", m.seq) } else { format!("MSG seq {} duplicate — ignored", m.seq) }
}

fn handle_ack(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>) -> String {
    let Some(owner) = owner else { return "ACK from unresolved sender — dropped".into() };
    let Some(seq) = decode_ack(env.body) else { return "ACK malformed".into() };
    let Some(room) = inner.rooms.get_mut(&env.room_id) else { return "ACK for unknown room".into() };
    if !room.is_rh { return "ACK ignored — we are not this room's RH".into(); }
    if !room.members.iter().any(|m| m.user_uuid == owner.user_uuid) {
        return "ACK from non-member — dropped".into();
    }
    let cur = room.member_acks.entry(owner.user_uuid).or_insert(0);
    if seq > *cur { *cur = seq; }
    format!("ACK from {} → seq {seq}", owner.label)
}

/// RH-side: replay messages with seq above a member's cursor to its UA address.
fn replay_to_member(inner: &mut Inner, room_id: &[u8; 16], member: [u8; 16], dev: &[u8; 16], app: &[u8; 16], outs: &mut Vec<Outbound>) {
    let Some(room) = inner.rooms.get(room_id) else { return };
    if !room.is_rh { return; }
    let cursor = *room.member_acks.get(&member).unwrap_or(&0);
    let mut pending: Vec<&ChatMsg> = room.messages.iter().filter(|m| m.seq > cursor).collect();
    pending.sort_by_key(|m| m.seq);
    for m in pending {
        let body = encode_msg(m.seq, &uuid_from_hex(&m.sender_user_hex), m.ts_ms, &[0u8; 16], &m.text);
        outs.push(Outbound { dest_device: *dev, dest_app: *app, msg_type: msg::MSG,
                             room_id: *room_id, body });
    }
}

fn handle_history_req(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "HISTORY_REQ from unresolved sender — dropped".into() };
    let Some((since, max)) = decode_history_req(env.body) else { return "HISTORY_REQ malformed".into() };
    let Some(room) = inner.rooms.get(&env.room_id) else { return "HISTORY_REQ for unknown room".into() };
    if !room.is_rh { return "HISTORY_REQ ignored — we are not this room's RH".into(); }
    if !room.members.iter().any(|m| m.user_uuid == owner.user_uuid) {
        return "HISTORY_REQ from non-member — dropped".into();
    }
    // Collect messages after `since`, chunked to fit one datagram.
    let mut msgs: Vec<&ChatMsg> = room.messages.iter().filter(|m| m.seq > since).collect();
    msgs.sort_by_key(|m| m.seq);
    let cap = if max == 0 { usize::MAX } else { max as usize };
    let mut entries: Vec<Vec<u8>> = Vec::new();
    let mut size = ENVELOPE_LEN + 8 + 8 + 1 + 2; // envelope + from/to/more/count
    let mut to = since;
    for m in msgs.into_iter().take(cap) {
        let e = encode_msg(m.seq, &uuid_from_hex(&m.sender_user_hex), m.ts_ms, &[0u8; 16], &m.text);
        if size + 2 + e.len() > MAX_APP_PAYLOAD && !entries.is_empty() { break; }
        size += 2 + e.len();
        to = m.seq;
        entries.push(e);
    }
    let highest = room.messages.iter().map(|m| m.seq).max().unwrap_or(0);
    let more = to < highest;
    let n = entries.len();
    let body = encode_history_resp(since, to, more, &entries);
    // Reply to the member's UA (resolved from the directory).
    if let Some((dev, app)) = inner.directory.get(&owner.user_uuid).and_then(|d| d.resolve_ua()) {
        outs.push(Outbound { dest_device: dev, dest_app: app, msg_type: msg::HISTORY_RESP,
                             room_id: env.room_id, body });
    }
    format!("HISTORY_REQ since {since} → replied {n} entries (more={more})")
}

fn handle_history_resp(inner: &mut Inner, env: &Envelope<'_>, owner: Option<&AppOwner>, outs: &mut Vec<Outbound>) -> String {
    let Some(owner) = owner else { return "HISTORY_RESP from unresolved sender — dropped".into() };
    let from_rh = inner.rooms.get(&env.room_id).map(|r| r.rh_device == owner.device_uuid).unwrap_or(false);
    if !from_rh { return "HISTORY_RESP not from our RH — dropped".into(); }
    let Some((_from, to, more, entries)) = decode_history_resp(env.body) else { return "HISTORY_RESP malformed".into() };
    let mut applied = 0;
    for e in &entries {
        if let Some(m) = decode_msg(e) {
            if apply_message(inner, &env.room_id, m.seq, m.sender_user, m.ts_ms, m.text) { applied += 1; }
        }
    }
    // Continue the chunked replay if there is more, then ACK our cursor.
    if more {
        to_rh(inner, &env.room_id, msg::HISTORY_REQ, encode_history_req(to, 0), outs);
    }
    ack_to_rh(inner, &env.room_id, outs);
    format!("HISTORY_RESP applied {applied} (more={more})")
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

async fn handle_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// A contact user we could invite to a room (for the create-room picker).
#[derive(Serialize)]
struct ContactUser {
    user_uuid_hex: String,
    alias: String,
    /// True if we can resolve their UA right now (else inviting will defer).
    ua_resolvable: bool,
}

#[derive(Serialize)]
struct ApiState {
    approved: bool,
    app_info: Option<AppInfo>,
    role: Role,
    ua: Option<UaRef>,
    attached_clients: Vec<AttachedClient>,
    attach: AttachState,
    own_user_uuid_hex: Option<String>,
    contacts: Vec<ContactUser>,
    rooms: Vec<RoomState>,
    destinations: Vec<Destination>,
    log: Vec<LogEntry>,
    last_fetch_ok: Option<u64>,
}

async fn handle_state(State(state): State<Arc<AppState>>) -> Json<ApiState> {
    let inner = state.inner.lock().unwrap();
    let contacts: Vec<ContactUser> = inner.directory.iter()
        .filter(|(_, d)| !d.own)
        .map(|(u, d)| ContactUser {
            user_uuid_hex: hex(u),
            alias: d.alias.clone(),
            ua_resolvable: d.resolve_ua().is_some(),
        })
        .collect();
    let mut rooms: Vec<RoomState> = inner.rooms.values().cloned().collect();
    rooms.sort_by(|a, b| a.name.cmp(&b.name));
    Json(ApiState {
        approved: inner.app_info.as_ref().map(|a| a.approved).unwrap_or(false),
        app_info: inner.app_info.clone(),
        role: inner.role,
        ua: inner.ua.clone(),
        own_user_uuid_hex: inner.own_user_uuid.map(|u| hex(&u)),
        contacts,
        rooms,
        attached_clients: inner.attached_clients.clone(),
        attach: inner.attach.clone(),
        destinations: inner.destinations.clone(),
        log: inner.log.clone(),
        last_fetch_ok: inner.last_fetch_ok,
    })
}

#[derive(Deserialize)]
struct SendRequest {
    dest_index: usize,
    text: String,
}

#[derive(Serialize)]
struct SendResponse {
    ok: bool,
    error: Option<String>,
}

async fn handle_send(
    State(state): State<Arc<AppState>>,
    Json(req): Json<SendRequest>,
) -> Json<SendResponse> {
    let (token, dest) = {
        let inner = state.inner.lock().unwrap();
        let Some(token) = inner.token else {
            return Json(SendResponse { ok: false, error: Some("not registered".into()) });
        };
        let Some(dest) = inner.destinations.get(req.dest_index).cloned() else {
            return Json(SendResponse { ok: false, error: Some("invalid destination index".into()) });
        };
        (token, dest)
    };

    // Phase 1: send a DEV_TEXT frame (zero room) so the pipe + envelope are
    // exercised without any room logic.
    let payload = encode_envelope(msg::DEV_TEXT, &ZERO_ROOM, &encode_dev_text(&req.text));
    if payload.len() > MAX_APP_PAYLOAD {
        return Json(SendResponse {
            ok: false,
            error: Some(format!("message too long ({} > {MAX_APP_PAYLOAD} bytes)", payload.len())),
        });
    }

    let pkt = build_send(&token, &dest.device_uuid, &dest.app_id, &payload);
    match state.ctrl_socket.send_to(&pkt, state.pnet_addr).await {
        Ok(_) => Json(SendResponse { ok: true, error: None }),
        Err(e) => Json(SendResponse { ok: false, error: Some(e.to_string()) }),
    }
}

async fn handle_refresh(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let token = state.inner.lock().unwrap().token;
    if let Some(token) = token {
        fetch_data(&state.ctrl_socket, state.pnet_addr, &token, &state.inner).await;
    }
    Json(serde_json::json!({ "ok": true }))
}

// ── Room management HTTP handlers ─────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateRoomRequest {
    name: String,
    /// Contact user uuids (hex) to invite.
    members: Vec<String>,
}

#[derive(Deserialize)]
struct RoomMemberRequest {
    room_id: String,
    user: String,
}

#[derive(Deserialize)]
struct RoomRequest {
    room_id: String,
}

async fn handle_room_create(
    State(state): State<Arc<AppState>>,
    Json(req): Json<CreateRoomRequest>,
) -> Json<SendResponse> {
    let members: Vec<[u8; 16]> = req.members.iter().filter_map(|h| hex_to_uuid(h)).collect();
    let mut outs: Vec<Outbound> = Vec::new();
    let (token, result) = {
        let mut inner = state.inner.lock().unwrap();
        let token = inner.token;
        let res = match inner.role {
            Role::UserAgent => {
                // We are the host's UA — become the RH directly.
                match create_room(&mut inner, &req.name, 0, 0, &members, None, &mut outs) {
                    Some(rid) => Ok(format!("created room {}…", &hex(&rid)[..8])),
                    None => Err("local identity not resolved yet".to_string()),
                }
            }
            Role::DataGuest => {
                // Delegate: send OPEN_ROOM to our UA, which mints + replies.
                match inner.ua.as_ref().and_then(|u| u.app_id.map(|a| (u.device_uuid, a))) {
                    Some((dev, app)) => {
                        let body = encode_open_room(&req.name, 0, 0, &members);
                        outs.push(Outbound { dest_device: dev, dest_app: app,
                            msg_type: msg::OPEN_ROOM, room_id: ZERO_ROOM, body });
                        Ok("OPEN_ROOM sent to our UA".to_string())
                    }
                    None => Err("no resolvable UA to host the room".to_string()),
                }
            }
            _ => Err("this node cannot create a room (role not resolved)".to_string()),
        };
        (token, res)
    };
    match (token, result) {
        (Some(token), Ok(msg)) => { flush(&state, &token, outs).await; Json(SendResponse { ok: true, error: Some(msg) }) }
        (_, Err(e)) => Json(SendResponse { ok: false, error: Some(e) }),
        (None, _) => Json(SendResponse { ok: false, error: Some("not registered".into()) }),
    }
}

async fn handle_room_add(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RoomMemberRequest>,
) -> Json<SendResponse> {
    room_member_op(state, &req.room_id, Some(&req.user), msg::ADD_MEMBER).await
}

async fn handle_room_remove(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RoomMemberRequest>,
) -> Json<SendResponse> {
    room_member_op(state, &req.room_id, Some(&req.user), msg::REMOVE_MEMBER).await
}

async fn handle_room_leave(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RoomRequest>,
) -> Json<SendResponse> {
    room_member_op(state, &req.room_id, None, msg::LEAVE).await
}

/// Drive an add/remove/leave from the local UI. When this node is the room's RH
/// it applies the change locally and fans updates out; otherwise it forwards the
/// control message to the room's RH (our UA).
async fn room_member_op(
    state: Arc<AppState>, room_hex: &str, user_hex: Option<&str>, msg_type: u8,
) -> Json<SendResponse> {
    let Some(room_id) = hex_to_uuid(room_hex) else {
        return Json(SendResponse { ok: false, error: Some("bad room id".into()) });
    };
    let target = match user_hex {
        Some(h) => match hex_to_uuid(h) {
            Some(u) => Some(u),
            None => return Json(SendResponse { ok: false, error: Some("bad user id".into()) }),
        },
        None => None,
    };

    let mut outs: Vec<Outbound> = Vec::new();
    let (token, result) = {
        let mut inner = state.inner.lock().unwrap();
        let token = inner.token;
        let Some(room) = inner.rooms.get(&room_id).cloned() else {
            return Json(SendResponse { ok: false, error: Some("unknown room".into()) });
        };
        let res = if room.is_rh {
            // Apply locally (we are the host's UA / RH).
            match msg_type {
                msg::ADD_MEMBER => local_add_member(&mut inner, &room_id, &target.unwrap(), &mut outs),
                msg::REMOVE_MEMBER => local_remove_member(&mut inner, &room_id, &target.unwrap(), &mut outs),
                msg::LEAVE => Err("host cannot LEAVE its own room".to_string()),
                _ => Err("unsupported op".to_string()),
            }
        } else {
            // Forward the control message to the RH (our UA).
            let body = match msg_type {
                msg::ADD_MEMBER | msg::REMOVE_MEMBER => target.unwrap().to_vec(),
                msg::LEAVE => Vec::new(),
                _ => return Json(SendResponse { ok: false, error: Some("unsupported op".into()) }),
            };
            outs.push(Outbound { dest_device: room.rh_device, dest_app: room.rh_app_id,
                msg_type, room_id, body });
            if msg_type == msg::LEAVE {
                inner.rooms.remove(&room_id); // optimistic local leave
            }
            Ok("forwarded to RH".to_string())
        };
        (token, res)
    };
    match (token, result) {
        (Some(token), Ok(m)) => { flush(&state, &token, outs).await; Json(SendResponse { ok: true, error: Some(m) }) }
        (_, Err(e)) => Json(SendResponse { ok: false, error: Some(e) }),
        (None, _) => Json(SendResponse { ok: false, error: Some("not registered".into()) }),
    }
}

/// RH-side add invoked from the local UI (the host operating on its own UA).
fn local_add_member(inner: &mut Inner, room_id: &[u8; 16], target: &[u8; 16], outs: &mut Vec<Outbound>) -> Result<String, String> {
    let room = inner.rooms.get(room_id).cloned().ok_or("unknown room")?;
    let dir = inner.directory.get(target).ok_or("target not a known contact")?;
    if dir.own { return Err("target is our own user".into()); }
    if room.members.iter().any(|m| m.user_uuid == *target) { return Err("already a member".into()); }
    let alias = dir.alias.clone();
    let new_member = Member { user_uuid_hex: hex(target), user_uuid: *target, alias: alias.clone(), present: false };
    fan_member_update(inner, &room, MEMBER_CHANGE_ADD, target, &alias, outs);
    inner.rooms.get_mut(room_id).unwrap().members.push(new_member.clone());
    let room_after = inner.rooms.get(room_id).cloned().unwrap();
    queue_invite(inner, &room_after, &new_member, outs);
    Ok(format!("added {alias}"))
}

fn local_remove_member(inner: &mut Inner, room_id: &[u8; 16], target: &[u8; 16], outs: &mut Vec<Outbound>) -> Result<String, String> {
    let room = inner.rooms.get(room_id).cloned().ok_or("unknown room")?;
    let removed = room.members.iter().find(|m| m.user_uuid == *target).cloned().ok_or("not a member")?;
    fan_member_update(inner, &room, MEMBER_CHANGE_REMOVE, target, &removed.alias, outs);
    inner.rooms.get_mut(room_id).unwrap().members.retain(|m| m.user_uuid != *target);
    Ok(format!("removed {}", removed.alias))
}

#[derive(Deserialize)]
struct PostRequest {
    room_id: String,
    text: String,
}

/// Post a message to a room. When we are the RH we order it locally and fan it
/// out; otherwise we send a POST to the RH (the ordered MSG comes back and is
/// applied like everyone else's — the round trip is the delivery confirmation).
async fn handle_room_post(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PostRequest>,
) -> Json<SendResponse> {
    let Some(room_id) = hex_to_uuid(&req.room_id) else {
        return Json(SendResponse { ok: false, error: Some("bad room id".into()) });
    };
    if req.text.trim().is_empty() {
        return Json(SendResponse { ok: false, error: Some("empty message".into()) });
    }
    let cid = gen_uuid();
    let mut outs: Vec<Outbound> = Vec::new();
    let (token, result) = {
        let mut inner = state.inner.lock().unwrap();
        let token = inner.token;
        let Some((is_rh, rh_dev, rh_app)) = inner.rooms.get(&room_id)
            .map(|r| (r.is_rh, r.rh_device, r.rh_app_id))
        else {
            return Json(SendResponse { ok: false, error: Some("unknown room".into()) });
        };
        let res = if is_rh {
            let sender = inner.own_user_uuid.unwrap_or([0u8; 16]);
            rh_accept_message(&mut inner, &room_id, sender, cid, req.text.clone(), &mut outs)
                .map(|seq| format!("posted as seq {seq}"))
        } else {
            outs.push(Outbound { dest_device: rh_dev, dest_app: rh_app, msg_type: msg::POST,
                                 room_id, body: encode_post(&cid, &req.text) });
            Ok("POST sent to RH".to_string())
        };
        (token, res)
    };
    match (token, result) {
        (Some(token), Ok(m)) => { flush(&state, &token, outs).await; Json(SendResponse { ok: true, error: Some(m) }) }
        (_, Err(e)) => Json(SendResponse { ok: false, error: Some(e) }),
        (None, _) => Json(SendResponse { ok: false, error: Some("not registered".into()) }),
    }
}

// ── Background data refresh loop ─────────────────────────────────────────────

async fn data_refresh_loop(state: Arc<AppState>) {
    loop {
        let connected = state.inner.lock().unwrap().app_info.is_some();
        let delay_secs = if connected { 30 } else { 5 };
        tokio::time::sleep(std::time::Duration::from_secs(delay_secs)).await;

        let token = state.inner.lock().unwrap().token;
        if let Some(token) = token {
            fetch_data(&state.ctrl_socket, state.pnet_addr, &token, &state.inner).await;
        }
    }
}

// ── Delegation: a DG attaches to its own User Agent ───────────────────────────

/// Interval at which a DG re-announces itself to its UA (presence keepalive).
const ATTACH_INTERVAL_SECS: u64 = 10;

/// When this node is a DataGuest with a resolved UA, periodically send
/// `CLIENT_ATTACH` so the UA knows this client is online and where to deliver.
/// A no-op for a UserAgent or a standby SG (they are not clients of anyone).
async fn attach_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(ATTACH_INTERVAL_SECS)).await;

        let target = {
            let inner = state.inner.lock().unwrap();
            if inner.role != Role::DataGuest { continue; }
            match (inner.token, &inner.ua) {
                // UA known and its pnet-chat app has synced into our snapshot.
                (Some(token), Some(ua)) => ua.app_id.map(|app| (token, ua.device_uuid, app)),
                _ => None,
            }
        };

        let Some((token, ua_device, ua_app)) = target else { continue; };
        let ok = send_frame(&state.ctrl_socket, state.pnet_addr, &token,
                            &ua_device, &ua_app, msg::CLIENT_ATTACH, &ZERO_ROOM, &[]).await;
        if ok {
            state.inner.lock().unwrap().attach.last_sent = Some(now_secs());
        }
    }
}

// ── RH room maintenance: re-invite members who have not HELLO'd yet ───────────

const ROOM_MAINT_INTERVAL_SECS: u64 = 15;

/// On each tick, for every room we host (RH), re-send INVITE to any member whose
/// UA has not yet announced presence. This heals discovery staleness — a member
/// whose app/SG-rank had not synced when the room was created becomes reachable
/// later (description.md, *Discovery is eventually consistent*).
async fn room_maintenance_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(ROOM_MAINT_INTERVAL_SECS)).await;
        let mut outs: Vec<Outbound> = Vec::new();
        let token = {
            let inner = state.inner.lock().unwrap();
            for room in inner.rooms.values() {
                if !room.is_rh { continue; }
                for m in &room.members {
                    if !m.present {
                        queue_invite(&inner, room, m, &mut outs);
                    }
                }
            }
            inner.token
        };
        if let Some(token) = token {
            if !outs.is_empty() {
                flush(&state, &token, outs).await;
            }
        }
    }
}

// ── RH reliability: retry unacked MSGs until every member's UA acks ───────────

const RH_RETRY_INTERVAL_SECS: u64 = 5;

/// On each tick, for every room we host, re-send any messages a member has not
/// yet acked to that member's UA. This is the reliable RH↔UA leg: a MSG lost in
/// the immediate fan-out, or a member that was offline/unresolvable when it was
/// posted, is healed here; the member's ACK trims the backlog so it converges
/// and stops. Idempotent apply on the member makes duplicate sends harmless.
async fn rh_retry_loop(state: Arc<AppState>) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(RH_RETRY_INTERVAL_SECS)).await;
        let mut outs: Vec<Outbound> = Vec::new();
        let token = {
            let inner = state.inner.lock().unwrap();
            for room in inner.rooms.values() {
                if !room.is_rh { continue; }
                let latest = room.messages.iter().map(|m| m.seq).max().unwrap_or(0);
                for m in &room.members {
                    let acked = *room.member_acks.get(&m.user_uuid).unwrap_or(&0);
                    if acked >= latest { continue; }
                    let Some((dev, app)) = inner.directory.get(&m.user_uuid).and_then(|d| d.resolve_ua())
                    else { continue };
                    for cm in room.messages.iter().filter(|x| x.seq > acked) {
                        let body = encode_msg(cm.seq, &uuid_from_hex(&cm.sender_user_hex), cm.ts_ms, &[0u8; 16], &cm.text);
                        outs.push(Outbound { dest_device: dev, dest_app: app, msg_type: msg::MSG,
                                             room_id: room.room_id, body });
                    }
                }
            }
            inner.token
        };
        if let Some(token) = token {
            if !outs.is_empty() {
                flush(&state, &token, outs).await;
            }
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let pnet_addr_str = std::env::var("PNET_ADDR").unwrap_or_else(|_| PNET_ADDR_DEFAULT.to_string());
    let pnet_addr: SocketAddr = tokio::net::lookup_host(&pnet_addr_str).await
        .unwrap_or_else(|e| panic!("invalid PNET_ADDR {pnet_addr_str:?}: {e}"))
        .next()
        .unwrap_or_else(|| panic!("PNET_ADDR {pnet_addr_str:?} resolved to no addresses"));

    // Port / token-file overrides let several instances coexist on one host
    // (each needs distinct push + ctrl ports and its own token file).
    let env_port = |name: &str, default: u16| -> u16 {
        std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
    };
    let http_port = env_port("PNET_HTTP_PORT", HTTP_PORT_DEFAULT);
    let push_port = env_port("PNET_CHAT_PUSH_PORT", PUSH_PORT_DEFAULT);
    let ctrl_port = env_port("PNET_CHAT_CTRL_PORT", CTRL_PORT_DEFAULT);
    let token_file = std::env::var("PNET_CHAT_TOKEN_FILE")
        .unwrap_or_else(|_| TOKEN_FILE_DEFAULT.to_string());

    let push_socket = Arc::new(
        UdpSocket::bind(format!("0.0.0.0:{push_port}"))
            .await
            .expect("failed to bind push UDP socket"),
    );
    let ctrl_socket = Arc::new(
        UdpSocket::bind(format!("0.0.0.0:{ctrl_port}"))
            .await
            .expect("failed to bind ctrl UDP socket"),
    );

    eprintln!("[startup] pnet-chat: push port {push_port}, ctrl port {ctrl_port}");

    const STARTUP_RETRIES: u32 = 4;
    const STARTUP_RETRY_DELAY_SECS: u64 = 2;

    // Try to reuse a saved token before registering fresh.
    let token = if let Some(saved) = load_token(&token_file) {
        eprintln!("[startup] found saved token {}, verifying...", hex(&saved));
        let inner_tmp = Mutex::new(Inner::new(Some(saved)));
        let mut verified = false;
        for attempt in 1..=STARTUP_RETRIES + 1 {
            fetch_data(&ctrl_socket, pnet_addr, &saved, &inner_tmp).await;
            if inner_tmp.lock().unwrap().app_info.is_some() {
                verified = true;
                break;
            }
            if attempt <= STARTUP_RETRIES {
                eprintln!("[startup] fetch attempt {attempt} failed, retrying in {STARTUP_RETRY_DELAY_SECS}s...");
                tokio::time::sleep(std::time::Duration::from_secs(STARTUP_RETRY_DELAY_SECS)).await;
            }
        }
        if verified {
            eprintln!("[startup] saved token is valid");
        } else {
            eprintln!("[startup] could not reach pnet — will keep retrying in background");
            eprintln!("[startup] if the token is invalid, delete {token_file} to re-register");
        }
        saved
    } else {
        eprintln!("[startup] no saved token, registering with pnet at {pnet_addr}...");
        match register(&ctrl_socket, pnet_addr, push_port).await {
            Some(t) => { save_token(&token_file, &t); eprintln!("[startup] token = {}", hex(&t)); t }
            None => {
                eprintln!("[startup] registration failed — is pnet running on {pnet_addr}?");
                std::process::exit(1);
            }
        }
    };

    let state = Arc::new(AppState {
        push_socket,
        ctrl_socket,
        pnet_addr,
        inner: Mutex::new(Inner::new(Some(token))),
    });

    fetch_data(&state.ctrl_socket, pnet_addr, &token, &state.inner).await;

    let approved = state.inner.lock().unwrap().app_info.as_ref().map(|a| a.approved).unwrap_or(false);
    if approved {
        eprintln!("[startup] app is approved");
    } else {
        eprintln!("[startup] app is NOT approved — visit the pnet admin UI to approve it");
    }

    tokio::spawn(push_receive_loop(state.clone()));
    tokio::spawn(data_refresh_loop(state.clone()));
    tokio::spawn(attach_loop(state.clone()));
    tokio::spawn(room_maintenance_loop(state.clone()));
    tokio::spawn(rh_retry_loop(state.clone()));

    let app = Router::new()
        .route("/", get(handle_index))
        .route("/api/state", get(handle_state))
        .route("/api/send", post(handle_send))
        .route("/api/refresh", post(handle_refresh))
        .route("/api/room/create", post(handle_room_create))
        .route("/api/room/add", post(handle_room_add))
        .route("/api/room/remove", post(handle_room_remove))
        .route("/api/room/leave", post(handle_room_leave))
        .route("/api/room/post", post(handle_room_post))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{http_port}"))
        .await
        .expect("failed to bind HTTP port");

    eprintln!("[startup] HTTP UI at http://127.0.0.1:{http_port}");
    axum::serve(listener, app).await.unwrap();
}

// ── Embedded HTML UI ──────────────────────────────────────────────────────────

const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>pNet Chat (Phase 1)</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: monospace; background: #1a1a1a; color: #e0e0e0; padding: 20px; max-width: 800px; margin: 0 auto; }
    h1 { color: #c39be0; margin-bottom: 4px; }
    .sub { color: #888; font-size: 0.8em; margin-bottom: 16px; }
    h2 { color: #a0c8a0; margin-bottom: 8px; font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px; }
    .status { background: #2a2a2a; border: 1px solid #444; padding: 10px 14px; border-radius: 4px; margin-bottom: 20px; }
    .status.approved { border-color: #5a8a5a; }
    .status.pending  { border-color: #8a7a3a; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 3px; font-size: 0.8em; margin-left: 8px; }
    .badge.ok   { background: #2a5a2a; color: #80e080; }
    .badge.warn { background: #5a4a1a; color: #e0c060; }
    .panel { background: #2a2a2a; border: 1px solid #444; border-radius: 4px; padding: 14px; margin-bottom: 20px; }
    .log { max-height: 320px; overflow-y: auto; }
    .entry { border-bottom: 1px solid #333; padding: 8px 0; }
    .entry:last-child { border-bottom: none; }
    .entry .sender { color: #c39be0; font-size: 0.85em; }
    .entry .kind   { color: #7ec8e3; font-size: 0.75em; margin-left: 6px; }
    .entry .detail { margin-top: 4px; white-space: pre-wrap; word-break: break-word; }
    .entry .time   { color: #666; font-size: 0.75em; float: right; }
    .empty { color: #666; font-style: italic; }
    .send-form { display: flex; flex-direction: column; gap: 10px; }
    select, textarea, button { font-family: monospace; font-size: 0.9em; }
    select   { background: #1a1a1a; color: #e0e0e0; border: 1px solid #555; padding: 6px 8px; border-radius: 3px; width: 100%; }
    textarea { background: #1a1a1a; color: #e0e0e0; border: 1px solid #555; padding: 6px 8px; border-radius: 3px; width: 100%; resize: vertical; min-height: 60px; }
    button { background: #3a2a5a; color: #c39be0; border: 1px solid #6a4a9a; padding: 8px 18px; border-radius: 3px; cursor: pointer; }
    button:hover { background: #4a3a6a; }
    button:disabled { opacity: 0.4; cursor: not-allowed; }
    .refresh-btn { float: right; font-size: 0.8em; padding: 4px 10px; }
    .error { color: #e08080; margin-top: 6px; font-size: 0.85em; }
    .gradetag { font-size: 0.7em; padding: 1px 5px; border-radius: 3px; background: #333; color: #9ab; }
    .chatlog { margin-top: 8px; max-height: 200px; overflow-y: auto; background: #1f1f1f; border: 1px solid #383838; border-radius: 4px; padding: 6px 8px; }
    .chatmsg { padding: 2px 0; font-size: 0.9em; word-break: break-word; }
    .chatmsg .seq { color: #555; font-size: 0.75em; }
    .chatmsg .who { font-weight: bold; }
    .compose { display: flex; gap: 6px; margin-top: 6px; }
    .compose input { flex: 1; background: #1a1a1a; color: #e0e0e0; border: 1px solid #555; padding: 6px 8px; border-radius: 3px; font-family: monospace; font-size: 0.9em; }
    .compose button { padding: 6px 14px; }
  </style>
</head>
<body>
  <h1>pNet Chat</h1>
  <div class="sub">Phase 3 — rooms: create on your UA (the RH), invite contacts, members HELLO back, add/remove/leave. No messages yet.</div>

  <div id="status" class="status">Connecting...</div>

  <div class="panel">
    <h2>Role &amp; Delegation</h2>
    <div id="role"><span class="empty">Resolving…</span></div>
  </div>

  <div class="panel">
    <h2>Rooms</h2>
    <div id="rooms"><span class="empty">No rooms yet.</span></div>
    <div style="margin-top:12px;border-top:1px solid #333;padding-top:12px">
      <h2>Create Room</h2>
      <div class="send-form">
        <input id="room-name" placeholder="Room name" style="background:#1a1a1a;color:#e0e0e0;border:1px solid #555;padding:6px 8px;border-radius:3px;width:100%;font-family:monospace">
        <div id="member-picker" style="font-size:0.85em"><span class="empty">No contacts to invite.</span></div>
        <div>
          <button id="create-btn" onclick="createRoom()">Create &amp; Invite</button>
          <span id="create-error" class="error"></span>
        </div>
      </div>
    </div>
  </div>

  <div class="panel">
    <h2>Inbound Frames <button class="refresh-btn" onclick="refresh()">Refresh</button></h2>
    <div id="log" class="log"><span class="empty">No frames yet.</span></div>
  </div>

  <div class="panel">
    <h2>Send DEV_TEXT Frame</h2>
    <div class="send-form">
      <select id="dest"><option value="">-- select destination --</option></select>
      <textarea id="text" placeholder="Type a test message..."></textarea>
      <div>
        <button id="send-btn" onclick="sendMsg()">Send</button>
        <span id="send-error" class="error"></span>
      </div>
    </div>
  </div>

  <script>
    function fmtTime(ts) { return new Date(ts * 1000).toLocaleTimeString(); }
    function escHtml(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

    async function loadState() {
      let s;
      try {
        const r = await fetch('/api/state');
        s = await r.json();
      } catch (e) { return; }

      const statusEl = document.getElementById('status');
      const syncLine = s.last_fetch_ok
        ? `<small style="color:#888;margin-top:4px;display:block">Last synced: ${new Date(s.last_fetch_ok * 1000).toLocaleTimeString()}</small>`
        : `<small style="color:#c08020;margin-top:4px;display:block">Last synced: never — waiting for pnet…</small>`;
      if (s.app_info) {
        const badge = s.approved
          ? '<span class="badge ok">APPROVED</span>'
          : '<span class="badge warn">PENDING APPROVAL</span>';
        statusEl.className = 'status ' + (s.approved ? 'approved' : 'pending');
        statusEl.innerHTML = `<strong>${escHtml(s.app_info.alias)}</strong> (id ${s.app_info.id_hex.slice(0,8)}…) ${badge}`;
        if (!s.approved) {
          statusEl.innerHTML += '<br><small style="color:#888;margin-top:4px;display:block">Approve this app in the pnet admin UI, then click Refresh.</small>';
        }
        statusEl.innerHTML += syncLine;
      } else {
        statusEl.className = 'status pending';
        statusEl.innerHTML = 'Not registered.' + syncLine;
      }

      // Role & delegation panel.
      const roleEl = document.getElementById('role');
      const roleNames = {
        UserAgent: 'User Agent — this user’s hub (top-ranked SG)',
        SgStandby: 'SG standby — a mirror/failover SG (not the top one)',
        DataGuest: 'Data Guest — thin client; delegates to its User Agent',
        Unknown:   'Unknown — waiting for first sync',
      };
      let roleHtml = `<div><strong>${escHtml(roleNames[s.role] || s.role)}</strong></div>`;
      if (s.role === 'UserAgent') {
        const n = s.attached_clients.length;
        roleHtml += `<div style="margin-top:6px">Attached clients (DGs): <strong>${n}</strong></div>`;
        if (n) roleHtml += '<div style="margin-top:4px">' + s.attached_clients.map(c =>
          `<div class="entry"><span class="time">${fmtTime(c.last_seen)}</span>${escHtml(c.label)} <span class="kind">${c.app_id_hex.slice(0,8)}…</span></div>`
        ).join('') + '</div>';
      } else if (s.role === 'DataGuest') {
        if (s.ua) {
          const appPart = s.ua.app_id_hex
            ? `app ${s.ua.app_id_hex.slice(0,8)}…`
            : '<span style="color:#c08020">app not synced yet — cannot attach</span>';
          roleHtml += `<div style="margin-top:6px">My User Agent: <strong>${escHtml(s.ua.label)}</strong> (r${s.ua.sg_rank}, ${appPart})</div>`;
          const attached = s.attach.last_ack && (Date.now()/1000 - s.attach.last_ack < 30);
          const badge = attached
            ? '<span class="badge ok">ATTACHED</span>'
            : '<span class="badge warn">NOT ATTACHED</span>';
          const sent = s.attach.last_sent ? `sent ${fmtTime(s.attach.last_sent)}` : 'not sent yet';
          const ack = s.attach.last_ack ? `acked ${fmtTime(s.attach.last_ack)}` : 'no ack yet';
          roleHtml += `<div style="margin-top:6px">${badge} <small style="color:#888">${sent}, ${ack}</small></div>`;
        } else {
          roleHtml += '<div style="margin-top:6px;color:#c08020">No User Agent — this user has no SG. (Members without an SG: deliver direct.)</div>';
        }
      }
      roleEl.innerHTML = roleHtml;

      // Rooms panel. Preserve a compose box the user is typing in across the
      // periodic re-render.
      const active = document.activeElement;
      const keepId = (active && active.id && active.id.startsWith('compose-')) ? active.id : null;
      const keepVal = keepId ? active.value : null;
      const roomsEl = document.getElementById('rooms');
      if (!s.rooms || s.rooms.length === 0) {
        roomsEl.innerHTML = '<span class="empty">No rooms yet.</span>';
      } else {
        roomsEl.innerHTML = s.rooms.map(r => {
          const tag = r.is_rh ? '<span class="badge ok">RH (host)</span>' : '<span class="badge warn">member</span>';
          const mem = r.members.map(m =>
            `<span class="gradetag" title="${m.user_uuid_hex}">${escHtml(m.alias)}${m.present ? ' ✓' : ' …'}` +
            (r.is_rh ? ` <a onclick="removeMember('${r.room_id_hex}','${m.user_uuid_hex}');return false" style="cursor:pointer;color:#e08080">✕</a>` : '') +
            `</span>`
          ).join(' ');
          const addCtl = r.is_rh ? `<a onclick="addPrompt('${r.room_id_hex}');return false" style="cursor:pointer;color:#7ec8e3;font-size:0.8em">+ add</a>` : '';
          const leaveCtl = !r.is_rh ? `<a onclick="leaveRoom('${r.room_id_hex}');return false" style="cursor:pointer;color:#e08080;font-size:0.8em">leave</a>` : '';
          const msgs = (r.messages && r.messages.length)
            ? r.messages.map(m =>
                `<div class="chatmsg"><span class="seq">#${m.seq}</span> <span class="who" style="color:${m.sender_is_self ? '#7ec8e3' : '#c39be0'}">${escHtml(m.sender_alias)}</span>: <span class="ctext">${escHtml(m.text)}</span></div>`
              ).join('')
            : '<span class="empty">No messages yet.</span>';
          return `<div class="entry">
            <strong>${escHtml(r.name)}</strong> ${tag}
            <span class="kind">${r.room_id_hex.slice(0,8)}…</span>
            <div style="margin-top:4px">members: ${mem || '<span class="empty">none</span>'} &nbsp; ${addCtl} ${leaveCtl}</div>
            <div class="chatlog">${msgs}</div>
            <div class="compose">
              <input id="compose-${r.room_id_hex}" placeholder="Message #${escHtml(r.name)}" onkeydown="if(event.key==='Enter'){postMsg('${r.room_id_hex}');}">
              <button onclick="postMsg('${r.room_id_hex}')">Send</button>
            </div>
          </div>`;
        }).join('');
        if (keepId) {
          const restored = document.getElementById(keepId);
          if (restored) { restored.value = keepVal; restored.focus(); }
        }
      }

      // Contact member picker for create-room.
      const pick = document.getElementById('member-picker');
      if (!s.contacts || s.contacts.length === 0) {
        pick.innerHTML = '<span class="empty">No contacts to invite.</span>';
      } else {
        pick.innerHTML = 'Invite: ' + s.contacts.map(c =>
          `<label style="margin-right:10px;white-space:nowrap">
            <input type="checkbox" class="mpick" value="${c.user_uuid_hex}"> ${escHtml(c.alias)}${c.ua_resolvable ? '' : ' <span style="color:#c08020">(UA not synced)</span>'}
          </label>`
        ).join(' ');
      }
      document.getElementById('create-btn').disabled = (s.role !== 'UserAgent' && s.role !== 'DataGuest');

      const logEl = document.getElementById('log');
      if (s.log.length === 0) {
        logEl.innerHTML = '<span class="empty">No frames yet.</span>';
      } else {
        logEl.innerHTML = s.log.slice().reverse().map(m =>
          `<div class="entry">
            <span class="time">${fmtTime(m.timestamp)}</span>
            <span class="sender">${escHtml(m.sender)}</span><span class="kind">${escHtml(m.kind)}</span>
            <div class="detail">${escHtml(m.detail)}</div>
          </div>`
        ).join('');
      }

      const destEl = document.getElementById('dest');
      const prev = destEl.value;
      destEl.innerHTML = '<option value="">-- select destination --</option>';
      s.destinations.forEach((d, i) => {
        const opt = document.createElement('option');
        opt.value = i;
        const rank = d.grade === 'SG' ? ` r${d.sg_rank}` : '';
        opt.textContent = `[${d.grade}${rank}] ${d.label}`;
        destEl.appendChild(opt);
      });
      if (prev !== '') destEl.value = prev;

      document.getElementById('send-btn').disabled = !s.approved;
    }

    async function refresh() {
      await fetch('/api/refresh', { method: 'POST' });
      await loadState();
    }

    async function sendMsg() {
      const destIdx = document.getElementById('dest').value;
      const text = document.getElementById('text').value.trim();
      const errEl = document.getElementById('send-error');
      errEl.textContent = '';

      if (destIdx === '') { errEl.textContent = 'Select a destination.'; return; }
      if (!text) { errEl.textContent = 'Enter a message.'; return; }

      const r = await fetch('/api/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ dest_index: parseInt(destIdx), text }),
      });
      const result = await r.json();
      if (result.ok) {
        document.getElementById('text').value = '';
      } else {
        errEl.textContent = result.error || 'Send failed.';
      }
    }

    async function postJson(url, body) {
      const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      return r.json();
    }

    async function createRoom() {
      const name = document.getElementById('room-name').value.trim();
      const errEl = document.getElementById('create-error');
      errEl.textContent = '';
      if (!name) { errEl.textContent = 'Enter a room name.'; return; }
      const members = Array.from(document.querySelectorAll('.mpick:checked')).map(c => c.value);
      const res = await postJson('/api/room/create', { name, members });
      if (res.ok) { document.getElementById('room-name').value = ''; await loadState(); }
      else errEl.textContent = res.error || 'Create failed.';
    }

    async function addPrompt(roomId) {
      const user = prompt('Contact user uuid (hex) to add:');
      if (!user) return;
      const res = await postJson('/api/room/add', { room_id: roomId, user: user.trim() });
      if (!res.ok) alert(res.error || 'Add failed.'); await loadState();
    }
    async function removeMember(roomId, user) {
      const res = await postJson('/api/room/remove', { room_id: roomId, user });
      if (!res.ok) alert(res.error || 'Remove failed.'); await loadState();
    }
    async function leaveRoom(roomId) {
      const res = await postJson('/api/room/leave', { room_id: roomId });
      if (!res.ok) alert(res.error || 'Leave failed.'); await loadState();
    }
    async function postMsg(roomId) {
      const el = document.getElementById('compose-' + roomId);
      const text = el.value.trim();
      if (!text) return;
      el.value = '';
      const res = await postJson('/api/room/post', { room_id: roomId, text });
      if (!res.ok) { alert(res.error || 'Post failed.'); el.value = text; }
      await loadState();
    }

    loadState();
    setInterval(loadState, 2000);
  </script>
</body>
</html>
"#;
