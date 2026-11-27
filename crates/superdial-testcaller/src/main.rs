//! superdial-testcaller: a headless MatrixRTC caller for integration tests.
//!
//! Output is JSON lines on stdout; logs go to stderr.

#![recursion_limit = "256"]

mod session;
mod wav;

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use matrix_sdk::{
    config::SyncSettings,
    deserialized_responses::RawAnySyncOrStrippedState,
    ruma::{
        api::client::room::create_room::v3::{Request as CreateRoom, RoomPreset},
        events::StateEventType,
        serde::Raw,
        OwnedRoomId, OwnedUserId,
    },
    Client, LoopCtrl,
};
use serde_json::{json, Value};
use superdial::{
    active_members, post_spoken_turn, Call, CallEvent, CallOptions, E2eeState, Homeserver, LkJwtDevIssuer, MediaEvent,
    SuperExchangeIssuer, TokenIssuer, TranscriptContent, CALL_MEMBER_EVENT,
};
use tokio::sync::RwLock;

#[derive(Parser)]
#[command(about = "Headless MatrixRTC test caller built on superdial")]
struct Cli {
    /// Homeserver base URL.
    #[arg(long)]
    homeserver: String,
    /// Localpart or full user ID.
    #[arg(long)]
    user: String,
    /// File holding the password (used only when there is no saved session).
    #[arg(long)]
    password_file: PathBuf,
    /// Directory for the crypto store and the saved session.
    #[arg(long)]
    data_dir: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, ValueEnum)]
enum IssuerKind {
    /// SuperExchange's token service (C5) at the discovered base, or --issuer-url.
    Superexchange,
    /// Development only: the rtc-spike's lk-jwt-service (/sfu/get).
    LkJwtDev,
}

#[derive(Subcommand)]
enum Command {
    /// Create an encrypted room whose members may publish call membership.
    CreateRoom {
        #[arg(long)]
        invite: Vec<OwnedUserId>,
    },
    /// Print the room's active call memberships.
    Members {
        #[arg(long)]
        room: OwnedRoomId,
    },
    /// Join the room's call, play and record audio, then hang up.
    Call {
        #[arg(long)]
        room: OwnedRoomId,
        #[arg(long, value_enum)]
        issuer: IssuerKind,
        #[arg(long)]
        issuer_url: Option<String>,
        /// WAV played (looped) as the microphone; silence if absent.
        #[arg(long)]
        play: Option<PathBuf>,
        /// Play the WAV once, --play-delay seconds after a peer's key arrives, then silence.
        #[arg(long)]
        play_once: bool,
        #[arg(long, default_value_t = 3.0)]
        play_delay: f64,
        /// Where to write what the peer said (48 kHz mono).
        #[arg(long)]
        record: Option<PathBuf>,
        /// How long to stay in the call once joined.
        #[arg(long, default_value_t = 25)]
        seconds: u64,
        /// Join rooms we are invited to.
        #[arg(long)]
        accept_invites: bool,
        /// Accept C2 transcripts from this agent's call device and post them as C3.
        #[arg(long)]
        transcripts_from: Option<OwnedUserId>,
        /// Send this text as a C2 transcript to the first peer, a few seconds after it joins.
        #[arg(long)]
        send_transcript: Option<String>,
    },
    /// Measure a recording.
    Analyze {
        #[arg(long)]
        wav: PathBuf,
        #[arg(long)]
        tone: f64,
    },
}

fn out(v: Value) {
    println!("{v}");
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| {
            "info,matrix_sdk=warn,matrix_sdk_base=warn,matrix_sdk_crypto=warn,livekit=warn,libwebrtc=warn".into()
        }))
        .init();
    let cli = Cli::parse();
    if let Command::Analyze { wav, tone } = &cli.command {
        let s = wav::read_mono_48k(wav)?;
        out(json!({"rms": wav::rms(&s), "tone_ratio": wav::tone_ratio(&s, *tone), "ms": s.len() / 48}));
        return Ok(());
    }
    let client = session::login(&cli.homeserver, &cli.user, &cli.password_file, &cli.data_dir).await?;
    client.sync_once(SyncSettings::default()).await?;
    match cli.command {
        Command::CreateRoom { invite } => create_room(&client, invite).await,
        Command::Members { room } => {
            out(json!({"members": members_json(&client, &room).await?}));
            Ok(())
        }
        Command::Call {
            room,
            issuer,
            issuer_url,
            play,
            play_once,
            play_delay,
            record,
            seconds,
            accept_invites,
            transcripts_from,
            send_transcript,
        } => {
            let args = CallArgs {
                room,
                issuer,
                issuer_url,
                play,
                play_once,
                play_delay,
                record,
                seconds,
                accept_invites,
                transcripts_from,
                send_transcript,
            };
            let ok = call(client, args).await?;
            std::process::exit(if ok { 0 } else { 1 });
        }
        Command::Analyze { .. } => unreachable!(),
    }
}

async fn create_room(client: &Client, invite: Vec<OwnedUserId>) -> Result<()> {
    let mut req = CreateRoom::new();
    req.invite = invite;
    req.preset = Some(RoomPreset::PrivateChat);
    req.name = Some("superdial test call".into());
    req.initial_state = vec![Raw::new(
        &json!({"type": "m.room.encryption", "state_key": "", "content": {"algorithm": "m.megolm.v1.aes-sha2"}}),
    )?
    .cast_unchecked()];
    req.power_level_content_override = Some(
        Raw::new(&json!({"events": {
            "org.matrix.msc3401.call.member": 0,
            "org.matrix.msc4143.rtc.member": 0,
            "org.matrix.msc4143.rtc.slot": 0,
        }}))?
        .cast_unchecked(),
    );
    let room = client.create_room(req).await?;
    out(json!({"room_id": room.room_id()}));
    Ok(())
}

async fn members_json(client: &Client, room_id: &OwnedRoomId) -> Result<Value> {
    let room = client.get_room(room_id).context("room not known")?;
    let raw = room.get_state_events(StateEventType::from(CALL_MEMBER_EVENT)).await?;
    let events: Vec<Value> = raw
        .into_iter()
        .filter_map(|r| match r {
            RawAnySyncOrStrippedState::Sync(r) => r.deserialize_as::<Value>().ok(),
            _ => None,
        })
        .collect();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    Ok(Value::Array(
        active_members(&events, now)
            .into_iter()
            .map(|m| json!({"user_id": m.user_id, "device_id": m.device_id, "membership_id": m.membership_id}))
            .collect(),
    ))
}

struct CallArgs {
    room: OwnedRoomId,
    issuer: IssuerKind,
    issuer_url: Option<String>,
    play: Option<PathBuf>,
    play_once: bool,
    play_delay: f64,
    record: Option<PathBuf>,
    seconds: u64,
    accept_invites: bool,
    transcripts_from: Option<OwnedUserId>,
    send_transcript: Option<String>,
}

async fn call(client: Client, a: CallArgs) -> Result<bool> {
    let me = client.user_id().context("not logged in")?.to_owned();
    let my_device = client.device_id().context("no device")?.to_owned();
    let slot: Arc<RwLock<Option<Call>>> = Arc::default();

    // Background sync: invites, to-device events into the call, membership refreshes, and
    // spoken-turn messages seen in the room.
    let (c2, s2, room_id, accept) = (client.clone(), slot.clone(), a.room.clone(), a.accept_invites);
    let sync = tokio::spawn(async move {
        let settings = SyncSettings::default().timeout(Duration::from_secs(5));
        let _ = c2
            .clone()
            .sync_with_callback(settings, move |resp| {
                let (client, slot, room_id) = (c2.clone(), s2.clone(), room_id.clone());
                async move {
                    if accept {
                        for id in resp.rooms.invited.keys() {
                            if let Some(r) = client.get_room(id) {
                                let joined = r.join().await.is_ok();
                                out(json!({"event": "invite_accepted", "room_id": id, "ok": joined}));
                            }
                        }
                    }
                    if let Some(update) = resp.rooms.joined.get(&room_id) {
                        for ev in &update.timeline.events {
                            if let Ok(v) = ev.raw().deserialize_as::<Value>() {
                                if let Some(m) = v["content"].get(superdial::SPOKEN_FIELD) {
                                    out(json!({"event": "spoken_message_seen", "sender": v["sender"], "body": v["content"]["body"], "marker": m}));
                                }
                            }
                        }
                    }
                    if let Some(call) = slot.read().await.as_ref() {
                        for ev in &resp.to_device {
                            call.handle_to_device(ev).await;
                        }
                        if let Err(e) = call.refresh_members().await {
                            tracing::warn!("refresh members: {e}");
                        }
                    }
                    LoopCtrl::Continue
                }
            })
            .await;
    });

    // Invites that arrived before the sync loop started (in the first sync).
    if a.accept_invites {
        for r in client.invited_rooms() {
            let ok = r.join().await.is_ok();
            out(json!({"event": "invite_accepted", "room_id": r.room_id(), "ok": ok}));
        }
    }
    // Wait until the room is joined (an invite may need accepting first).
    let t0 = Instant::now();
    while client.get_room(&a.room).map(|r| r.state()) != Some(matrix_sdk::RoomState::Joined) {
        if t0.elapsed() > Duration::from_secs(60) {
            bail!("room {} not joined after 60 s", a.room);
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    let issuer: Option<Arc<dyn TokenIssuer>> = match (a.issuer, a.issuer_url.as_deref()) {
        (IssuerKind::Superexchange, None) => None,
        (IssuerKind::Superexchange, Some(u)) => Some(Arc::new(SuperExchangeIssuer::new(u))),
        (IssuerKind::LkJwtDev, url) => {
            let base = match url {
                Some(u) => u.to_owned(),
                None => Homeserver::from_client(&client)?.livekit_service_url().await?,
            };
            Some(Arc::new(LkJwtDevIssuer::new(&base)))
        }
    };
    let options = CallOptions {
        transcripts_from: a.transcripts_from.clone(),
        ..CallOptions::default()
    };
    // Reachable from the sync loop before joining: peers' keys can arrive as soon as our
    // membership is published.
    let (call, mut events) = Call::new(&client, &a.room, options)?;
    *slot.write().await = Some(call.clone());
    call.join(issuer).await?;
    let grant = call.grant().context("joined call has a grant")?;
    let own_identity = grant.identity.clone();
    out(
        json!({"event": "joined", "identity": own_identity, "livekit_room": grant.livekit_room,
               "livekit_room_by_hash": superdial::livekit_room_name(a.room.as_str()), "delay_id": call.delay_id()}),
    );

    // Microphone: loop the WAV (or silence) until the call ends; or, with --play-once, silence
    // until a peer's key has arrived and --play-delay has passed, the WAV once, then silence.
    let (peer_keyed_tx, mut peer_keyed) = tokio::sync::watch::channel(false);
    let mic = {
        let samples = match &a.play {
            Some(p) => wav::read_mono_48k(p)?,
            None => vec![0; 48_000],
        };
        let (call, once, delay) = (call.clone(), a.play_once, Duration::from_secs_f64(a.play_delay));
        tokio::spawn(async move {
            let quiet = vec![0i16; 4_800];
            if once {
                while !*peer_keyed.borrow_and_update() {
                    if call.capture(&quiet).await.is_err() {
                        return;
                    }
                }
                let until = tokio::time::Instant::now() + delay;
                while tokio::time::Instant::now() < until {
                    if call.capture(&quiet).await.is_err() {
                        return;
                    }
                }
                out(json!({"event": "playing", "ms": samples.len() / 48}));
                if let Err(e) = call.capture(&samples).await {
                    tracing::warn!("capture: {e}");
                    return;
                }
                out(json!({"event": "played"}));
            }
            loop {
                let chunk = if once { &quiet } else { &samples };
                if let Err(e) = call.capture(chunk).await {
                    tracing::warn!("capture: {e}");
                    break;
                }
            }
        })
    };

    let deadline = tokio::time::Instant::now() + Duration::from_secs(a.seconds);
    let mut e2ee: BTreeMap<String, &'static str> = BTreeMap::new();
    let mut recorded: Vec<i16> = Vec::new();
    let mut first_peer: Option<(superdial::CallMember, tokio::time::Instant)> = None;
    let mut transcript_sent = false;
    let mut transcripts_posted = 0u32;
    let mut audio_frames: BTreeMap<String, u64> = BTreeMap::new();
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            _ = tick.tick() => {
                if let (Some(text), Some((peer, at)), false) = (&a.send_transcript, &first_peer, transcript_sent) {
                    if at.elapsed() > Duration::from_secs(4) {
                        let t = TranscriptContent {
                            room_id: a.room.to_string(),
                            turn_id: uuid::Uuid::new_v4().to_string(),
                            text: text.clone(),
                            lang: Some("en".into()),
                            audio_ms: 1234,
                            sent_ts: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64,
                        };
                        let r = call.send_transcript(peer, &t).await;
                        out(json!({"event": "transcript_sent", "to": peer.identity(), "turn_id": t.turn_id, "ok": r.is_ok(), "error": r.err().map(|e| e.to_string())}));
                        transcript_sent = true;
                    }
                }
            }
            ev = events.recv() => {
                let Some(ev) = ev else { break };
                match ev {
                    CallEvent::Media(MediaEvent::Audio { identity, samples }) => {
                        *audio_frames.entry(identity).or_default() += 1;
                        recorded.extend_from_slice(&samples);
                    }
                    CallEvent::Media(MediaEvent::E2ee { identity, state }) => {
                        out(json!({"event": "e2ee", "identity": identity, "state": state.as_str()}));
                        e2ee.insert(identity, state.as_str());
                    }
                    CallEvent::MemberJoined(m) => {
                        out(json!({"event": "member_joined", "identity": m.identity()}));
                        if first_peer.is_none() {
                            first_peer = Some((m, tokio::time::Instant::now()));
                        }
                    }
                    CallEvent::MemberLeft(m) => out(json!({"event": "member_left", "identity": m.identity()})),
                    CallEvent::KeySent { to } => out(json!({"event": "key_sent", "to": to.identity()})),
                    CallEvent::KeyRegistered { identity } => {
                        if identity != own_identity {
                            let _ = peer_keyed_tx.send(true);
                        }
                        out(json!({"event": "key_registered", "identity": identity}))
                    }
                    // Its attributes at connect, `superdial.state` included; the library sends
                    // them again as an `attributes` change right after.
                    CallEvent::Media(MediaEvent::ParticipantConnected { identity, attributes }) => {
                        out(json!({"event": "participant_connected", "identity": identity, "attributes": attributes}))
                    }
                    CallEvent::Media(MediaEvent::AttributesChanged { identity, attributes }) => {
                        out(json!({"event": "attributes", "identity": identity, "attributes": attributes}))
                    }
                    CallEvent::KeyIgnored { reason } => out(json!({"event": "key_ignored", "reason": reason})),
                    CallEvent::Transcript { from, content } => {
                        out(json!({"event": "transcript_received", "from": from.identity(), "text": content.text, "turn_id": content.turn_id}));
                        let room = client.get_room(&a.room).context("room")?;
                        match post_spoken_turn(&room, &content, &from.user_id).await {
                            Ok(id) => { transcripts_posted += 1; out(json!({"event": "spoken_turn_posted", "event_id": id})) }
                            Err(e) => out(json!({"event": "spoken_turn_post_failed", "error": e.to_string()})),
                        }
                    }
                    CallEvent::Media(other) => out(json!({"event": "media", "detail": format!("{other:?}")})),
                }
            }
        }
    }

    mic.abort();
    call.leave().await.context("leave")?;
    out(json!({"event": "left"}));
    // Let sync bring our own leave back, then check the room state.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let members = members_json(&client, &a.room).await?;
    let cleared = !members
        .as_array()
        .map(|a| {
            a.iter()
                .any(|m| m["user_id"] == me.as_str() && m["device_id"] == my_device.as_str())
        })
        .unwrap_or(false);
    *slot.write().await = None;
    sync.abort();

    if let Some(path) = &a.record {
        wav::write_mono_48k(path, &recorded)?;
    }
    let remote: Vec<_> = e2ee.iter().filter(|(id, _)| **id != own_identity).collect();
    let all_ok = !remote.is_empty() && remote.iter().all(|(_, s)| **s == E2eeState::Ok.as_str());
    out(json!({"summary": {
        "identity": own_identity,
        "e2ee": e2ee,
        "audio_frames": audio_frames,
        "recorded_ms": recorded.len() / 48,
        "rms": wav::rms(&recorded),
        "transcripts_posted": transcripts_posted,
        "membership_cleared": cleared,
        "remaining_members": members,
    }}));
    let transcripts_ok = a.transcripts_from.is_none() || transcripts_posted > 0;
    Ok(all_ok && cleared && transcripts_ok)
}
