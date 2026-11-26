//! The real [`CallLayer`]: one [`DeviceSession`] per agent call device, and per room one
//! `superdial::Call` with its [`conversation`](crate::conversation).
//!
//! A room's entry stays in the map until its call has left completely. A PUT for a room whose
//! call is still leaving starts the new call only after the old one has cleared its membership,
//! because both use the same state key.

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

use async_trait::async_trait;
use matrix_sdk::ruma::{OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId};
use superdial::{Call, CallEvent, CallMember, CallOptions, MediaEvent, TokenIssuer, TranscriptContent};
use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    api::{ApiError, CallLayer, JoinRequest, Reply},
    conversation::{self, AgentState, CallIo, ConversationConfig, Input, STATE_ATTRIBUTE},
    hub::TurnCanceller,
    session::{store_dir, DeviceSession},
    speech::{Synthesizer, Transcriber},
};

/// How long a call waits for sync to bring its room.
const ROOM_WAIT: Duration = Duration::from_secs(30);
/// How long C6 DELETE waits for the leave.
const LEAVE_WAIT: Duration = Duration::from_secs(30);
/// How long a reply waits for the conversation to take it.
const REPLY_WAIT: Duration = Duration::from_secs(10);

/// What the agent is built from.
pub struct AgentDeps {
    pub state_dir: PathBuf,
    pub store_passphrase: Option<String>,
    pub stt: Arc<dyn Transcriber>,
    pub tts: Arc<dyn Synthesizer>,
    pub hub: Arc<dyn TurnCanceller>,
    /// `None`: SuperExchange's token service, discovered from the homeserver.
    pub issuer: Option<Arc<dyn TokenIssuer>>,
    pub log_text: bool,
}

struct Entry {
    id: u64,
    agent: OwnedUserId,
    inputs: mpsc::UnboundedSender<Input>,
    leaving: Arc<AtomicBool>,
    done: watch::Receiver<bool>,
}

type SessionKey = (OwnedUserId, OwnedDeviceId);

/// superdial-agent's calls.
pub struct Agent {
    deps: AgentDeps,
    calls: Mutex<HashMap<OwnedRoomId, Entry>>,
    sessions: tokio::sync::Mutex<HashMap<SessionKey, Arc<DeviceSession>>>,
    next_id: AtomicU64,
    /// Ourselves, for the tasks [`CallLayer::join`] spawns.
    me: Weak<Agent>,
}

impl Agent {
    pub fn new(deps: AgentDeps) -> Arc<Agent> {
        Arc::new_cyclic(|me| Agent {
            deps,
            calls: Mutex::default(),
            sessions: tokio::sync::Mutex::default(),
            next_id: AtomicU64::new(1),
            me: me.clone(),
        })
    }

    /// Leaves every call (on shutdown), cancelling their waiting turns.
    pub async fn shutdown(&self) {
        let rooms: Vec<OwnedRoomId> = self.calls.lock().expect("calls").keys().cloned().collect();
        futures::future::join_all(rooms.iter().map(|r| self.leave(r))).await;
    }

    /// The device's session, started if needed, with a new call for `room` routed to it.
    async fn attach(
        &self,
        req: &JoinRequest,
        room: &RoomId,
    ) -> anyhow::Result<(Arc<DeviceSession>, Call, mpsc::UnboundedReceiver<CallEvent>)> {
        let device: OwnedDeviceId = req.device.device_id.as_str().into();
        let key = (req.agent_user_id.clone(), device.clone());
        let mut sessions = self.sessions.lock().await;
        let session = match sessions.get(&key) {
            Some(s) => s.clone(),
            None => {
                let dir = store_dir(&self.deps.state_dir, &req.agent_user_id, &device);
                let s = DeviceSession::start(
                    &req.homeserver_url,
                    &req.agent_user_id,
                    &device,
                    &req.device.access_token,
                    dir,
                    self.deps.store_passphrase.as_deref(),
                )
                .await?;
                tracing::info!(agent = %req.agent_user_id, device = %device, "device session started");
                sessions.insert(key, s.clone());
                s
            }
        };
        let (call, events) = Call::new(session.client(), room, CallOptions::default())?;
        session.add_call(room.to_owned(), call.clone()).await;
        Ok((session, call, events))
    }

    /// Stops routing to the room's call; stops the device's session when it was its last call.
    async fn detach(&self, key: &SessionKey, room: &RoomId) {
        let mut sessions = self.sessions.lock().await;
        let Some(session) = sessions.get(key).cloned() else {
            return;
        };
        if session.remove_call(room).await == 0 {
            session.stop().await;
            sessions.remove(key);
            tracing::info!(agent = %key.0, device = %key.1, "device session stopped");
        }
    }

    /// One call, from joining to gone.
    async fn run_call(
        &self,
        room: OwnedRoomId,
        req: JoinRequest,
        inputs: mpsc::UnboundedReceiver<Input>,
        inputs_tx: mpsc::UnboundedSender<Input>,
        previous: Option<watch::Receiver<bool>>,
    ) {
        if let Some(mut prev) = previous {
            let _ = prev.wait_for(|done| *done).await;
        }
        let key: SessionKey = (req.agent_user_id.clone(), req.device.device_id.as_str().into());
        match self.attach(&req, &room).await {
            Ok((session, call, events)) => {
                self.join_and_converse(&room, &req, &session, call, events, inputs, inputs_tx)
                    .await;
                self.detach(&key, &room).await;
            }
            Err(e) => tracing::error!(room = %room, agent = %req.agent_user_id, "could not start the call device: {e}"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn join_and_converse(
        &self,
        room: &OwnedRoomId,
        req: &JoinRequest,
        session: &DeviceSession,
        call: Call,
        mut events: mpsc::UnboundedReceiver<CallEvent>,
        inputs: mpsc::UnboundedReceiver<Input>,
        inputs_tx: mpsc::UnboundedSender<Input>,
    ) {
        if !session.wait_for_room(room, ROOM_WAIT).await {
            tracing::error!(room = %room, agent = %req.agent_user_id, "the agent's call device does not see the room joined");
            return;
        }
        let started = std::time::Instant::now();
        if let Err(e) = call.join(self.deps.issuer.clone()).await {
            tracing::error!(room = %room, agent = %req.agent_user_id, "joining the call failed: {e}");
            let _ = call.leave().await;
            return;
        }
        let own = call.grant().map(|g| g.identity.clone()).unwrap_or_default();
        tracing::info!(room = %room, identity = %own, join_ms = started.elapsed().as_millis() as u64, "joined the call");

        // CallEvents → conversation inputs.
        let glue = {
            let (call, tx) = (call.clone(), inputs_tx);
            tokio::spawn(async move {
                while let Some(ev) = events.recv().await {
                    let input = match ev {
                        CallEvent::MemberJoined(m) | CallEvent::MemberLeft(m) => {
                            tracing::info!(room = %call.room_id(), member = %m.identity(), "call membership changed");
                            Input::MembersChanged {
                                count: call.members().await.len(),
                            }
                        }
                        CallEvent::KeyRegistered { identity } if identity != own => Input::PeerReady,
                        CallEvent::KeyIgnored { reason } => {
                            tracing::warn!(room = %call.room_id(), reason, "call key ignored");
                            continue;
                        }
                        CallEvent::Media(MediaEvent::Audio { identity, samples }) => Input::Audio { identity, samples },
                        CallEvent::Media(MediaEvent::Disconnected { reason }) => Input::Disconnected { reason },
                        CallEvent::Media(MediaEvent::E2ee { identity, state }) => {
                            tracing::info!(room = %call.room_id(), identity, state = state.as_str(), "media encryption");
                            continue;
                        }
                        _ => continue,
                    };
                    if tx.send(input).is_err() {
                        break;
                    }
                }
            })
        };

        let mut cfg = ConversationConfig::new(room.clone(), req.voice.clone(), req.stt_language.clone());
        cfg.log_text = self.deps.log_text;
        let deps = conversation::Deps {
            io: Arc::new(CallAdapter { call: call.clone() }),
            stt: self.deps.stt.clone(),
            tts: self.deps.tts.clone(),
            hub: self.deps.hub.clone(),
        };
        let reason = conversation::run(cfg, deps, inputs).await;
        glue.abort();
        tracing::info!(room = %room, ?reason, call_s = started.elapsed().as_secs(), "call ended");
    }

    fn finished(&self, room: &RoomId, id: u64) {
        let mut calls = self.calls.lock().expect("calls");
        if calls.get(room).is_some_and(|e| e.id == id) {
            calls.remove(room);
        }
    }
}

#[async_trait]
impl CallLayer for Agent {
    async fn join(&self, room: OwnedRoomId, req: JoinRequest) -> Result<(), ApiError> {
        let Some(this) = self.me.upgrade() else {
            return Err(ApiError::Internal("shutting down".into()));
        };
        let mut calls = self.calls.lock().expect("calls");
        let previous = match calls.get(&room) {
            Some(e) if e.agent != req.agent_user_id => {
                return Err(ApiError::Conflict(format!("{room} is in a call with {}", e.agent)));
            }
            Some(e) if !e.leaving.load(Ordering::SeqCst) => return Ok(()),
            Some(e) => Some(e.done.clone()),
            None => None,
        };
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (inputs_tx, inputs) = mpsc::unbounded_channel();
        let (done_tx, done) = watch::channel(false);
        let leaving = Arc::new(AtomicBool::new(false));
        calls.insert(
            room.clone(),
            Entry {
                id,
                agent: req.agent_user_id.clone(),
                inputs: inputs_tx.clone(),
                leaving: leaving.clone(),
                done,
            },
        );
        drop(calls);
        tokio::spawn(async move {
            this.run_call(room.clone(), req, inputs, inputs_tx, previous).await;
            leaving.store(true, Ordering::SeqCst);
            this.finished(&room, id);
            let _ = done_tx.send(true);
        });
        Ok(())
    }

    async fn leave(&self, room: &RoomId) {
        let (tx, mut done) = {
            let calls = self.calls.lock().expect("calls");
            let Some(e) = calls.get(room) else { return };
            e.leaving.store(true, Ordering::SeqCst);
            (e.inputs.clone(), e.done.clone())
        };
        let (ack, acked) = oneshot::channel();
        if tx.send(Input::Leave { done: ack }).is_ok() {
            let _ = tokio::time::timeout(LEAVE_WAIT, acked).await;
        }
        let _ = tokio::time::timeout(LEAVE_WAIT, done.wait_for(|d| *d)).await;
    }

    async fn reply(&self, room: &RoomId, turn_id: &str, reply: Reply) -> Result<(), ApiError> {
        let tx = {
            let calls = self.calls.lock().expect("calls");
            calls
                .get(room)
                .map(|e| e.inputs.clone())
                .ok_or_else(|| ApiError::NotFound(format!("no call in {room}")))?
        };
        let (done, result) = oneshot::channel();
        let gone = || ApiError::NotFound(format!("the call in {room} has ended"));
        tx.send(Input::Reply {
            turn_id: turn_id.to_owned(),
            reply,
            done,
        })
        .map_err(|_| gone())?;
        match tokio::time::timeout(REPLY_WAIT, result).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(gone()),
            Err(_) => Err(ApiError::Internal("the call did not take the reply in time".into())),
        }
    }

    fn calls(&self) -> usize {
        self.calls
            .lock()
            .expect("calls")
            .values()
            .filter(|e| !e.leaving.load(Ordering::SeqCst))
            .count()
    }
}

/// [`CallIo`] over a joined `superdial::Call`.
struct CallAdapter {
    call: Call,
}

#[async_trait]
impl CallIo for CallAdapter {
    async fn members(&self) -> Vec<CallMember> {
        self.call.members().await
    }

    async fn send_transcript(&self, to: &CallMember, t: &TranscriptContent) -> Result<(), String> {
        self.call.send_transcript(to, t).await.map_err(|e| e.to_string())
    }

    async fn play(&self, pcm48k: &[i16]) -> Result<(), String> {
        self.call.capture(pcm48k).await.map_err(|e| e.to_string())
    }

    async fn set_state(&self, state: AgentState) -> Result<(), String> {
        let media = self.call.media().ok_or("not connected")?;
        media
            .set_attribute(STATE_ATTRIBUTE, state.as_str())
            .await
            .map_err(|e| e.to_string())
    }

    async fn leave(&self) -> Result<(), String> {
        self.call.leave().await.map_err(|e| e.to_string())
    }
}
