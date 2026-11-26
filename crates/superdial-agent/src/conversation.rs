//! One call, from the agent's side: hear each thing a caller says, send its transcript to the
//! caller's call device (C2), speak the hub's reply, show `superdial.state`, and on leaving
//! cancel any turn still waiting. An actor: all turn state lives in [`run`]'s task, fed by
//! [`Input`]s; the joined call is behind [`CallIo`], so this runs against fakes in tests.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use matrix_sdk::ruma::OwnedRoomId;
use superdial::{CallMember, TranscriptContent};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

use crate::{
    api::{ApiError, Outcome, Reply},
    audio,
    hub::TurnCanceller,
    speech::{Heard, SpeechError, Synthesizer, Transcriber},
    textprep::{speakable, SPOKEN_CAP},
    turns::{TurnConfig, TurnDetector, Utterance},
};

/// The LiveKit participant attribute the caller reads the agent's state from.
pub const STATE_ATTRIBUTE: &str = "superdial.state";

/// The agent's state, as shown to the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    Connecting,
    Listening,
    Thinking,
    Speaking,
}

impl AgentState {
    pub fn as_str(self) -> &'static str {
        match self {
            AgentState::Connecting => "connecting",
            AgentState::Listening => "listening",
            AgentState::Thinking => "thinking",
            AgentState::Speaking => "speaking",
        }
    }
}

/// The joined call, as the conversation uses it.
#[async_trait]
pub trait CallIo: Send + Sync {
    /// The other participants' devices.
    async fn members(&self) -> Vec<CallMember>;
    /// Sends a C2 transcript to one member's call device.
    async fn send_transcript(&self, to: &CallMember, t: &TranscriptContent) -> Result<(), String>;
    /// Plays 48 kHz mono PCM as the agent's voice; returns when it is queued.
    async fn play(&self, pcm48k: &[i16]) -> Result<(), String>;
    /// Sets [`STATE_ATTRIBUTE`].
    async fn set_state(&self, state: AgentState) -> Result<(), String>;
    /// Leaves the call.
    async fn leave(&self) -> Result<(), String>;
}

/// What the conversation is told.
#[derive(Debug)]
pub enum Input {
    /// 10 ms of a remote participant's decoded audio.
    Audio { identity: String, samples: Vec<i16> },
    /// A peer's media key is registered: the call can carry audio.
    PeerReady,
    /// The number of other participants changed.
    MembersChanged { count: usize },
    /// LiveKit dropped the connection.
    Disconnected { reason: String },
    /// The hub's reply to a spoken turn.
    Reply {
        turn_id: String,
        reply: Reply,
        done: oneshot::Sender<Result<(), ApiError>>,
    },
    /// The hub asked us to leave (C6 DELETE).
    Leave { done: oneshot::Sender<()> },
}

/// Why a conversation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndReason {
    /// C6 DELETE.
    Deleted,
    /// The last other participant left.
    Hangup,
    /// LiveKit disconnected.
    Disconnected,
    /// Nobody joined within the grace period.
    NoPeer,
    /// The input channel closed (the process is shutting down).
    InputsClosed,
}

/// How a conversation behaves.
#[derive(Debug, Clone)]
pub struct ConversationConfig {
    pub room_id: OwnedRoomId,
    pub voice: String,
    pub stt_language: Option<String>,
    pub turn: TurnConfig,
    /// How long to wait for a first participant before leaving.
    pub no_peer_grace: Duration,
    /// Audio stays ignored this long after playback is queued (LiveKit's queue drains after).
    pub speak_tail: Duration,
    /// A turn with no reply after this long is forgotten.
    pub turn_timeout: Duration,
    /// Development only: log transcript and reply text.
    pub log_text: bool,
}

impl ConversationConfig {
    pub fn new(room_id: OwnedRoomId, voice: String, stt_language: Option<String>) -> Self {
        Self {
            room_id,
            voice,
            stt_language,
            turn: TurnConfig::default(),
            no_peer_grace: Duration::from_secs(60),
            speak_tail: Duration::from_secs(1),
            turn_timeout: Duration::from_secs(15 * 60),
            log_text: false,
        }
    }
}

/// The services a conversation uses.
#[derive(Clone)]
pub struct Deps {
    pub io: Arc<dyn CallIo>,
    pub stt: Arc<dyn Transcriber>,
    pub tts: Arc<dyn Synthesizer>,
    pub hub: Arc<dyn TurnCanceller>,
}

/// Runs one call's conversation until it ends, then cancels waiting turns and leaves.
pub async fn run(cfg: ConversationConfig, deps: Deps, mut inputs: mpsc::UnboundedReceiver<Input>) -> EndReason {
    let (internal_tx, mut internal) = mpsc::unbounded_channel();
    let mut c = Conversation {
        cfg,
        deps,
        internal: internal_tx,
        detectors: HashMap::new(),
        pending: HashMap::new(),
        queue: VecDeque::new(),
        phase: Phase::Idle,
        player: None,
        shown: None,
        ready: false,
        had_peer: false,
        started: Instant::now(),
    };
    c.show_state().await;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let (reason, done) = loop {
        let step = tokio::select! {
            input = inputs.recv() => match input {
                Some(input) => c.on_input(input).await,
                None => Step::End(EndReason::InputsClosed, None),
            },
            Some(ev) = internal.recv() => c.on_internal(ev).await,
            _ = tick.tick() => c.on_tick().await,
        };
        if let Step::End(reason, done) = step {
            break (reason, done);
        }
    };
    c.finish(reason).await;
    if let Some(done) = done {
        let _ = done.send(());
    }
    reason
}

enum Step {
    Continue,
    End(EndReason, Option<oneshot::Sender<()>>),
}

/// What spawned work reports back.
enum Internal {
    Heard {
        identity: String,
        audio_ms: u64,
        took: Duration,
        result: Result<Heard, SpeechError>,
    },
    Synthesized {
        took: Duration,
        result: Result<Vec<i16>, SpeechError>,
    },
    Played {
        result: Result<(), String>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Synthesizing,
    Playing,
}

/// A turn whose transcript went to the caller and whose reply has not come yet.
struct Pending {
    chunks: BTreeMap<u64, String>,
    since: Instant,
}

struct Conversation {
    cfg: ConversationConfig,
    deps: Deps,
    internal: mpsc::UnboundedSender<Internal>,
    detectors: HashMap<String, TurnDetector>,
    pending: HashMap<String, Pending>,
    /// Reply text waiting to be spoken, in arrival order.
    queue: VecDeque<String>,
    phase: Phase,
    player: Option<JoinHandle<()>>,
    shown: Option<AgentState>,
    ready: bool,
    had_peer: bool,
    started: Instant,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Conversation {
    fn room(&self) -> &str {
        self.cfg.room_id.as_str()
    }

    fn state(&self) -> AgentState {
        if !self.ready {
            AgentState::Connecting
        } else if self.phase == Phase::Playing {
            AgentState::Speaking
        } else if !self.pending.is_empty() || !self.queue.is_empty() || self.phase == Phase::Synthesizing {
            AgentState::Thinking
        } else {
            AgentState::Listening
        }
    }

    async fn show_state(&mut self) {
        let state = self.state();
        if self.shown == Some(state) {
            return;
        }
        self.shown = Some(state);
        tracing::info!(room = self.room(), state = state.as_str(), "state");
        if let Err(e) = self.deps.io.set_state(state).await {
            tracing::warn!(room = self.room(), "could not set {STATE_ATTRIBUTE}: {e}");
        }
    }

    async fn on_input(&mut self, input: Input) -> Step {
        match input {
            Input::Audio { identity, samples } => {
                self.ready = true;
                if self.phase != Phase::Playing {
                    let cfg = &self.cfg.turn;
                    let detector = self
                        .detectors
                        .entry(identity.clone())
                        .or_insert_with(|| TurnDetector::new(cfg.clone()));
                    if let Some(u) = detector.push(&samples) {
                        self.transcribe(identity, u);
                    }
                }
            }
            Input::PeerReady => self.ready = true,
            Input::MembersChanged { count } => {
                tracing::info!(room = self.room(), count, "participants");
                if count > 0 {
                    self.had_peer = true;
                } else if self.had_peer {
                    return Step::End(EndReason::Hangup, None);
                }
            }
            Input::Disconnected { reason } => {
                tracing::warn!(room = self.room(), reason, "LiveKit disconnected");
                return Step::End(EndReason::Disconnected, None);
            }
            Input::Reply { turn_id, reply, done } => {
                let _ = done.send(self.on_reply(&turn_id, reply));
            }
            Input::Leave { done } => return Step::End(EndReason::Deleted, Some(done)),
        }
        self.show_state().await;
        Step::Continue
    }

    fn transcribe(&self, identity: String, u: Utterance) {
        tracing::info!(room = self.room(), audio_ms = u.audio_ms, "utterance");
        let (stt, tx) = (self.deps.stt.clone(), self.internal.clone());
        let language = self.cfg.stt_language.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let pcm = audio::to_16k(&u.samples);
            let result = stt.transcribe(&pcm, language.as_deref()).await;
            let _ = tx.send(Internal::Heard {
                identity,
                audio_ms: u.audio_ms,
                took: started.elapsed(),
                result,
            });
        });
    }

    fn on_reply(&mut self, turn_id: &str, reply: Reply) -> Result<(), ApiError> {
        let Some(turn) = self.pending.get_mut(turn_id) else {
            return Err(ApiError::NotFound(format!("no turn {turn_id} waiting in this call")));
        };
        match (reply.outcome, reply.is_final) {
            (Outcome::Ok, false) => {
                turn.chunks.insert(reply.seq, reply.text);
            }
            (Outcome::Ok, true) => {
                let mut turn = self.pending.remove(turn_id).expect("present");
                turn.chunks.insert(reply.seq, reply.text);
                let text = speakable(&turn.chunks.into_values().collect::<String>(), SPOKEN_CAP);
                if self.cfg.log_text {
                    tracing::info!(room = self.room(), turn_id, text, "reply to speak");
                }
                if !text.is_empty() {
                    self.queue.push_back(text);
                    self.speak_next();
                }
            }
            (outcome, _) => {
                tracing::info!(room = self.room(), turn_id, ?outcome, "turn ended without a reply");
                self.pending.remove(turn_id);
            }
        }
        Ok(())
    }

    fn speak_next(&mut self) {
        if self.phase != Phase::Idle {
            return;
        }
        let Some(text) = self.queue.pop_front() else { return };
        self.phase = Phase::Synthesizing;
        let (tts, tx, voice) = (self.deps.tts.clone(), self.internal.clone(), self.cfg.voice.clone());
        tokio::spawn(async move {
            let started = Instant::now();
            let result = tts.synthesize(&text, &voice).await;
            let _ = tx.send(Internal::Synthesized {
                took: started.elapsed(),
                result,
            });
        });
    }

    async fn on_internal(&mut self, ev: Internal) -> Step {
        match ev {
            Internal::Heard {
                identity,
                audio_ms,
                took,
                result,
            } => self.on_heard(identity, audio_ms, took, result).await,
            Internal::Synthesized { took, result } => match result {
                Ok(pcm) => {
                    tracing::info!(
                        room = self.room(),
                        tts_ms = took.as_millis() as u64,
                        audio_ms = pcm.len() as u64 / 48,
                        "speaking"
                    );
                    self.phase = Phase::Playing;
                    self.detectors.values_mut().for_each(TurnDetector::reset);
                    let (io, tx, tail) = (self.deps.io.clone(), self.internal.clone(), self.cfg.speak_tail);
                    self.player = Some(tokio::spawn(async move {
                        let result = io.play(&pcm).await;
                        tokio::time::sleep(tail).await;
                        let _ = tx.send(Internal::Played { result });
                    }));
                }
                Err(e) => {
                    tracing::warn!(room = self.room(), "text to speech failed: {e}");
                    self.phase = Phase::Idle;
                    self.speak_next();
                }
            },
            Internal::Played { result } => {
                if let Err(e) = result {
                    tracing::warn!(room = self.room(), "playing the reply failed: {e}");
                }
                self.player = None;
                self.phase = Phase::Idle;
                self.detectors.values_mut().for_each(TurnDetector::reset);
                self.speak_next();
            }
        }
        self.show_state().await;
        Step::Continue
    }

    async fn on_heard(&mut self, identity: String, audio_ms: u64, took: Duration, result: Result<Heard, SpeechError>) {
        let stt_ms = took.as_millis() as u64;
        let heard = match result {
            Ok(h) => h,
            Err(e) => {
                tracing::warn!(room = self.room(), stt_ms, "speech to text failed: {e}");
                return;
            }
        };
        let text = heard.text.trim();
        if text.is_empty() {
            tracing::info!(room = self.room(), stt_ms, "nothing heard");
            return;
        }
        let Some(to) = self
            .deps
            .io
            .members()
            .await
            .into_iter()
            .find(|m| m.identity() == identity)
        else {
            tracing::warn!(
                room = self.room(),
                "heard a participant who is not a call member; no transcript sent"
            );
            return;
        };
        let content = TranscriptContent {
            room_id: self.room().to_owned(),
            turn_id: uuid::Uuid::new_v4().to_string(),
            text: text.to_owned(),
            lang: heard.language.or_else(|| self.cfg.stt_language.clone()),
            audio_ms,
            sent_ts: now_ms(),
        };
        if self.cfg.log_text {
            tracing::info!(room = self.room(), turn_id = content.turn_id, text, "heard");
        }
        match self.deps.io.send_transcript(&to, &content).await {
            Ok(()) => {
                tracing::info!(
                    room = self.room(),
                    turn_id = content.turn_id,
                    to = %to.identity(),
                    stt_ms,
                    audio_ms,
                    chars = text.chars().count(),
                    "transcript sent"
                );
                self.pending.insert(
                    content.turn_id,
                    Pending {
                        chunks: BTreeMap::new(),
                        since: Instant::now(),
                    },
                );
            }
            Err(e) => tracing::warn!(room = self.room(), "could not send the transcript: {e}"),
        }
    }

    async fn on_tick(&mut self) -> Step {
        if !self.had_peer && self.started.elapsed() >= self.cfg.no_peer_grace {
            tracing::info!(room = self.room(), "nobody came; leaving");
            return Step::End(EndReason::NoPeer, None);
        }
        let timeout = self.cfg.turn_timeout;
        let before = self.pending.len();
        self.pending.retain(|_, p| p.since.elapsed() < timeout);
        if self.pending.len() < before {
            tracing::warn!(
                room = self.room(),
                dropped = before - self.pending.len(),
                "turns timed out without a reply"
            );
        }
        self.show_state().await;
        Step::Continue
    }

    /// Cancels every turn still waiting, then leaves the call.
    async fn finish(&mut self, reason: EndReason) {
        if let Some(p) = self.player.take() {
            p.abort();
        }
        let room = self.cfg.room_id.clone();
        for turn_id in self.pending.keys() {
            match self.deps.hub.cancel(&room, turn_id).await {
                Ok(()) => tracing::info!(room = %room, turn_id, "turn cancelled at the hub"),
                Err(e) => tracing::warn!(room = %room, turn_id, "could not cancel the turn: {e}"),
            }
        }
        self.pending.clear();
        if let Err(e) = self.deps.io.leave().await {
            tracing::warn!(room = %room, "leaving the call: {e}");
        }
        tracing::info!(room = %room, ?reason, "left the call");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        hub::HubError,
        turns::tests::{silence, tone},
    };
    use matrix_sdk::ruma::RoomId;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Mutex,
    };

    const CALLER: &str = "@caller:s:PHONE";
    const OTHER: &str = "@other:s:LAPTOP";

    fn member(identity: &str) -> CallMember {
        let (user, device) = identity.rsplit_once(':').unwrap();
        CallMember {
            user_id: user.try_into().unwrap(),
            device_id: device.into(),
            membership_id: format!("m-{device}"),
        }
    }

    #[derive(Default)]
    struct FakeIo {
        members: Mutex<Vec<CallMember>>,
        transcripts: Mutex<Vec<(String, TranscriptContent)>>,
        states: Mutex<Vec<AgentState>>,
        played: Mutex<Vec<usize>>,
        play_ms: u64,
        fail_send: AtomicBool,
        left: AtomicUsize,
    }

    #[async_trait]
    impl CallIo for FakeIo {
        async fn members(&self) -> Vec<CallMember> {
            self.members.lock().unwrap().clone()
        }
        async fn send_transcript(&self, to: &CallMember, t: &TranscriptContent) -> Result<(), String> {
            if self.fail_send.load(Ordering::SeqCst) {
                return Err("olm".into());
            }
            self.transcripts.lock().unwrap().push((to.identity(), t.clone()));
            Ok(())
        }
        async fn play(&self, pcm: &[i16]) -> Result<(), String> {
            tokio::time::sleep(Duration::from_millis(self.play_ms)).await;
            self.played.lock().unwrap().push(pcm.len());
            Ok(())
        }
        async fn set_state(&self, s: AgentState) -> Result<(), String> {
            self.states.lock().unwrap().push(s);
            Ok(())
        }
        async fn leave(&self) -> Result<(), String> {
            self.left.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FakeStt {
        reply: Mutex<Result<Heard, u16>>,
        calls: Mutex<Vec<(usize, Option<String>)>>,
    }

    #[async_trait]
    impl Transcriber for FakeStt {
        async fn transcribe(&self, pcm: &[i16], language: Option<&str>) -> Result<Heard, SpeechError> {
            self.calls
                .lock()
                .unwrap()
                .push((pcm.len(), language.map(str::to_owned)));
            self.reply.lock().unwrap().clone().map_err(|status| SpeechError::Http {
                status,
                detail: String::new(),
            })
        }
    }

    #[derive(Default)]
    struct FakeTts {
        texts: Mutex<Vec<(String, String)>>,
        fail: AtomicBool,
    }

    #[async_trait]
    impl Synthesizer for FakeTts {
        async fn synthesize(&self, text: &str, voice: &str) -> Result<Vec<i16>, SpeechError> {
            self.texts.lock().unwrap().push((text.to_owned(), voice.to_owned()));
            if self.fail.load(Ordering::SeqCst) {
                return Err(SpeechError::Transport("down".into()));
            }
            Ok(vec![1; 4800])
        }
    }

    #[derive(Default)]
    struct FakeHub {
        cancels: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl TurnCanceller for FakeHub {
        async fn cancel(&self, room_id: &RoomId, turn_id: &str) -> Result<(), HubError> {
            self.cancels
                .lock()
                .unwrap()
                .push((room_id.to_string(), turn_id.to_owned()));
            Ok(())
        }
    }

    struct Harness {
        io: Arc<FakeIo>,
        stt: Arc<FakeStt>,
        tts: Arc<FakeTts>,
        hub: Arc<FakeHub>,
        tx: mpsc::UnboundedSender<Input>,
        task: JoinHandle<EndReason>,
    }

    struct Ended {
        reason: EndReason,
        io: Arc<FakeIo>,
        hub: Arc<FakeHub>,
    }

    fn config() -> ConversationConfig {
        let mut c = ConversationConfig::new("!r:s".try_into().unwrap(), "af_heart".into(), Some("en".into()));
        c.speak_tail = Duration::ZERO;
        c
    }

    fn start_with(cfg: ConversationConfig, play_ms: u64, members: &[&str]) -> Harness {
        let io = Arc::new(FakeIo {
            play_ms,
            ..FakeIo::default()
        });
        *io.members.lock().unwrap() = members.iter().map(|m| member(m)).collect();
        let stt = Arc::new(FakeStt {
            reply: Mutex::new(Ok(Heard {
                text: "What is the capital of France?".into(),
                language: Some("en".into()),
            })),
            calls: Mutex::default(),
        });
        let tts = Arc::new(FakeTts::default());
        let hub = Arc::new(FakeHub::default());
        let deps = Deps {
            io: io.clone(),
            stt: stt.clone(),
            tts: tts.clone(),
            hub: hub.clone(),
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(run(cfg, deps, rx));
        let h = Harness {
            io,
            stt,
            tts,
            hub,
            tx,
            task,
        };
        h.tx.send(Input::MembersChanged { count: members.len() }).unwrap();
        h
    }

    fn start(members: &[&str]) -> Harness {
        start_with(config(), 0, members)
    }

    impl Harness {
        fn say(&self, identity: &str) {
            let mut audio = tone(1000, 8000.0);
            audio.extend(silence(900));
            for f in audio.chunks(480) {
                self.tx
                    .send(Input::Audio {
                        identity: identity.into(),
                        samples: f.to_vec(),
                    })
                    .unwrap();
            }
        }

        async fn reply(
            &self,
            turn_id: &str,
            seq: u64,
            text: &str,
            is_final: bool,
            outcome: Outcome,
        ) -> Result<(), ApiError> {
            let (done, rx) = oneshot::channel();
            self.tx
                .send(Input::Reply {
                    turn_id: turn_id.into(),
                    reply: Reply {
                        seq,
                        text: text.into(),
                        is_final,
                        outcome,
                    },
                    done,
                })
                .unwrap();
            rx.await.unwrap()
        }

        /// Sends C6 DELETE's input, waits for the end, and returns what the tests look at.
        async fn leave(self) -> Ended {
            let (done, rx) = oneshot::channel();
            self.tx.send(Input::Leave { done }).unwrap();
            rx.await.unwrap();
            Ended {
                reason: self.task.await.unwrap(),
                io: self.io,
                hub: self.hub,
            }
        }

        fn states(&self) -> Vec<AgentState> {
            self.io.states.lock().unwrap().clone()
        }

        fn turns(&self) -> Vec<String> {
            self.io
                .transcripts
                .lock()
                .unwrap()
                .iter()
                .map(|(_, t)| t.turn_id.clone())
                .collect()
        }
    }

    async fn eventually(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..300 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("timed out waiting for {what}");
    }

    fn is_uuid_v4(s: &str) -> bool {
        regex::Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
            .unwrap()
            .is_match(s)
    }

    use AgentState::*;

    #[tokio::test]
    async fn states_move_connecting_listening_thinking_speaking_listening() {
        let h = start(&[CALLER]);
        h.tx.send(Input::PeerReady).unwrap();
        eventually("listening", || h.states() == [Connecting, Listening]).await;
        h.say(CALLER);
        eventually("thinking", || h.states().last() == Some(&Thinking)).await;
        let turn = h.turns()[0].clone();
        h.reply(&turn, 0, "Paris.", true, Outcome::Ok).await.unwrap();
        eventually("back to listening", || h.states().len() == 5).await;
        assert_eq!(h.states(), [Connecting, Listening, Thinking, Speaking, Listening]);
        assert_eq!(*h.io.played.lock().unwrap(), vec![4800]);
        assert_eq!(*h.tts.texts.lock().unwrap(), vec![("Paris.".into(), "af_heart".into())]);
    }

    #[tokio::test]
    async fn transcript_goes_to_speaking_device_only() {
        let h = start(&[OTHER, CALLER]);
        h.say(CALLER);
        eventually("a transcript", || h.turns().len() == 1).await;
        let (to, t) = h.io.transcripts.lock().unwrap()[0].clone();
        assert_eq!(to, CALLER);
        assert!(is_uuid_v4(&t.turn_id), "{}", t.turn_id);
        assert_eq!(t.room_id, "!r:s");
        assert_eq!(t.text, "What is the capital of France?");
        assert_eq!(t.lang.as_deref(), Some("en"));
        assert!((1800..=2100).contains(&t.audio_ms), "{}", t.audio_ms);
        assert!(t.sent_ts > 1_700_000_000_000);
        // Audio first makes the call usable even before a key event is seen.
        assert_eq!(h.states()[..2], [Connecting, Listening]);
    }

    #[tokio::test]
    async fn two_utterances_are_two_turns() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("first", || h.turns().len() == 1).await;
        h.say(CALLER);
        eventually("second", || h.turns().len() == 2).await;
        let t = h.turns();
        assert_ne!(t[0], t[1]);
    }

    #[tokio::test]
    async fn stt_language_is_passed_and_audio_is_16k() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("stt called", || h.stt.calls.lock().unwrap().len() == 1).await;
        let (len, lang) = h.stt.calls.lock().unwrap()[0].clone();
        assert_eq!(lang.as_deref(), Some("en"));
        assert!((1800 * 16..=2100 * 16).contains(&len), "{len}");
    }

    #[tokio::test]
    async fn empty_transcript_sends_nothing() {
        let h = start(&[CALLER]);
        *h.stt.reply.lock().unwrap() = Ok(Heard {
            text: "".into(),
            language: None,
        });
        h.say(CALLER);
        eventually("stt called", || h.stt.calls.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(h.turns().is_empty());
        assert!(!h.states().contains(&Thinking));
    }

    #[tokio::test]
    async fn stt_error_returns_to_listening() {
        let h = start(&[CALLER]);
        *h.stt.reply.lock().unwrap() = Err(503);
        h.say(CALLER);
        eventually("stt called", || h.stt.calls.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(h.turns().is_empty());
        assert_eq!(h.states().last(), Some(&Listening));
        // And it still hears the next thing said.
        *h.stt.reply.lock().unwrap() = Ok(Heard {
            text: "Again?".into(),
            language: None,
        });
        h.say(CALLER);
        eventually("a transcript", || h.turns().len() == 1).await;
        // No language heard: the configured one is reported.
        assert_eq!(h.io.transcripts.lock().unwrap()[0].1.lang.as_deref(), Some("en"));
    }

    #[tokio::test]
    async fn failed_c2_send_makes_no_turn() {
        let h = start(&[CALLER]);
        h.io.fail_send.store(true, Ordering::SeqCst);
        h.say(CALLER);
        eventually("stt called", || h.stt.calls.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!h.states().contains(&Thinking));
        let e = h.leave().await;
        assert!(e.hub.cancels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reply_is_made_speakable() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        let turn = h.turns()[0].clone();
        h.reply(
            &turn,
            0,
            "## Answer\n**Paris** is the [capital](https://x).",
            true,
            Outcome::Ok,
        )
        .await
        .unwrap();
        eventually("tts", || h.tts.texts.lock().unwrap().len() == 1).await;
        assert_eq!(h.tts.texts.lock().unwrap()[0].0, "Answer.\nParis is the capital.");
    }

    #[tokio::test]
    async fn reply_while_speaking_is_queued() {
        let h = start_with(config(), 300, &[CALLER]);
        h.say(CALLER);
        eventually("first turn", || h.turns().len() == 1).await;
        h.say(CALLER);
        eventually("second turn", || h.turns().len() == 2).await;
        let t = h.turns();
        h.reply(&t[0], 0, "One.", true, Outcome::Ok).await.unwrap();
        eventually("speaking", || h.states().last() == Some(&Speaking)).await;
        h.reply(&t[1], 0, "Two.", true, Outcome::Ok).await.unwrap();
        eventually("both played", || h.io.played.lock().unwrap().len() == 2).await;
        let texts: Vec<String> = h.tts.texts.lock().unwrap().iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(texts, ["One.", "Two."]);
        eventually("listening", || h.states().last() == Some(&Listening)).await;
    }

    #[tokio::test]
    async fn audio_ignored_while_speaking() {
        let h = start_with(config(), 400, &[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        let turn = h.turns()[0].clone();
        h.reply(&turn, 0, "Paris.", true, Outcome::Ok).await.unwrap();
        eventually("speaking", || h.states().last() == Some(&Speaking)).await;
        h.say(CALLER);
        eventually("listening", || h.states().last() == Some(&Listening)).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(h.stt.calls.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancelled_reply_returns_to_listening() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        let turn = h.turns()[0].clone();
        h.reply(&turn, 0, "", true, Outcome::Cancelled).await.unwrap();
        eventually("listening", || h.states().last() == Some(&Listening)).await;
        assert!(h.tts.texts.lock().unwrap().is_empty());
        // The turn is gone: a late reply to it is not found.
        assert!(matches!(
            h.reply(&turn, 0, "late", true, Outcome::Ok).await,
            Err(ApiError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn error_reply_returns_to_listening() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        h.reply(&h.turns()[0], 0, "", true, Outcome::Error).await.unwrap();
        eventually("listening", || h.states().last() == Some(&Listening)).await;
        assert!(h.tts.texts.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn non_final_chunks_are_joined_on_final() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        let turn = h.turns()[0].clone();
        h.reply(&turn, 1, "is Paris.", false, Outcome::Ok).await.unwrap();
        h.reply(&turn, 0, "The capital ", false, Outcome::Ok).await.unwrap();
        assert!(h.tts.texts.lock().unwrap().is_empty());
        assert_eq!(h.states().last(), Some(&Thinking));
        h.reply(&turn, 2, "", true, Outcome::Ok).await.unwrap();
        eventually("tts", || h.tts.texts.lock().unwrap().len() == 1).await;
        assert_eq!(h.tts.texts.lock().unwrap()[0].0, "The capital is Paris.");
    }

    #[tokio::test]
    async fn unknown_turn_reply_is_not_found() {
        let h = start(&[CALLER]);
        let r = h
            .reply("0b6c7c4e-3f0e-4c1b-9a43-6f1d2a9e5b77", 0, "x", true, Outcome::Ok)
            .await;
        assert!(matches!(r, Err(ApiError::NotFound(_))));
    }

    #[tokio::test]
    async fn tts_error_returns_to_listening() {
        let h = start(&[CALLER]);
        h.tts.fail.store(true, Ordering::SeqCst);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        h.reply(&h.turns()[0], 0, "Paris.", true, Outcome::Ok).await.unwrap();
        eventually("tts tried", || h.tts.texts.lock().unwrap().len() == 1).await;
        eventually("listening", || h.states().last() == Some(&Listening)).await;
        assert!(h.io.played.lock().unwrap().is_empty());
        assert!(!h.states().contains(&Speaking));
    }

    #[tokio::test]
    async fn leave_cancels_pending_turns_at_hub() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        let turn = h.turns()[0].clone();
        let e = h.leave().await;
        assert_eq!(e.reason, EndReason::Deleted);
        assert_eq!(*e.hub.cancels.lock().unwrap(), vec![("!r:s".to_owned(), turn)]);
        assert_eq!(e.io.left.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn leave_without_pending_turn_cancels_nothing() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        h.reply(&h.turns()[0], 0, "Paris.", true, Outcome::Ok).await.unwrap();
        let e = h.leave().await;
        assert!(e.hub.cancels.lock().unwrap().is_empty());
        assert_eq!(e.io.left.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn last_member_leaving_hangs_up_and_cancels() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        h.tx.send(Input::MembersChanged { count: 0 }).unwrap();
        assert_eq!(h.task.await.unwrap(), EndReason::Hangup);
        assert_eq!(h.hub.cancels.lock().unwrap().len(), 1);
        assert_eq!(h.io.left.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_leave_before_first_peer() {
        let mut cfg = config();
        cfg.no_peer_grace = Duration::from_secs(30);
        let h = start_with(cfg, 0, &[]);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(!h.task.is_finished());
        assert_eq!(h.io.left.load(Ordering::SeqCst), 0);
        h.tx.send(Input::MembersChanged { count: 1 }).unwrap();
        h.tx.send(Input::MembersChanged { count: 0 }).unwrap();
        assert_eq!(h.task.await.unwrap(), EndReason::Hangup);
    }

    #[tokio::test]
    async fn leaves_when_no_peer_within_grace() {
        let mut cfg = config();
        cfg.no_peer_grace = Duration::from_millis(200);
        let h = start_with(cfg, 0, &[]);
        let reason = tokio::time::timeout(Duration::from_secs(3), h.task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(reason, EndReason::NoPeer);
        assert_eq!(h.io.left.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn disconnect_leaves() {
        let h = start(&[CALLER]);
        h.say(CALLER);
        eventually("turn", || h.turns().len() == 1).await;
        h.tx.send(Input::Disconnected {
            reason: "ServerShutdown".into(),
        })
        .unwrap();
        assert_eq!(h.task.await.unwrap(), EndReason::Disconnected);
        assert_eq!(h.hub.cancels.lock().unwrap().len(), 1);
        assert_eq!(h.io.left.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn stale_turn_is_forgotten() {
        let mut cfg = config();
        cfg.turn_timeout = Duration::from_millis(100);
        let h = start_with(cfg, 0, &[CALLER]);
        h.say(CALLER);
        eventually("thinking", || h.states().last() == Some(&Thinking)).await;
        eventually("listening again", || h.states().last() == Some(&Listening)).await;
        let e = h.leave().await;
        assert!(e.hub.cancels.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn audio_from_a_non_member_makes_no_turn() {
        let h = start(&[OTHER]);
        h.say(CALLER);
        eventually("stt called", || h.stt.calls.lock().unwrap().len() == 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(h.turns().is_empty());
    }
}
