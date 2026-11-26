//! A joined call: ties discovery, the token issuer, membership with its delayed leave, keys,
//! transcripts and media together. The application owns the matrix-sdk client and its sync
//! loop, and feeds the call with to-device events and membership refreshes.

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use livekit::{e2ee::key_provider::KeyProvider, prelude::ParticipantIdentity};

use matrix_sdk::{
    deserialized_responses::{ProcessedToDeviceEvent, RawAnySyncOrStrippedState},
    ruma::{
        events::{room::member::MembershipState, AnyToDeviceEventContent, StateEventType},
        serde::Raw,
        OwnedDeviceId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
    },
    Client,
};
use matrix_sdk_base::crypto::CollectStrategy;
use serde_json::Value;
use tokio::sync::{mpsc, Mutex, OnceCell};

use crate::{
    contract::{
        livekit_identity, membership_content, membership_state_key, spoken_message_content, SpokenMarker,
        TranscriptContent, CALL_MEMBER_EVENT, DELAYED_LEAVE_MS, KEYS_EVENT, RESTART_EVERY, TRANSCRIPT_EVENT,
    },
    delayed_leave::{spawn_restart_loop, RestartLoop},
    homeserver::{DelayedAction, Homeserver},
    issuer::{DelayedLeaveRequest, LiveKitGrant, SuperExchangeIssuer, TokenIssuer, TokenRequest},
    keys::{key_message, parse_key_event, CallKey, KeyRejection},
    media::{new_key_provider, Media, MediaEvent},
    membership::{active_members, CallMember},
    state::{CallState, TranscriptRejection},
    Error,
};

/// How a call behaves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallOptions {
    /// The delayed leave, armed before the membership is published.
    pub delayed_leave_ms: u64,
    /// How often the delayed leave is restarted.
    pub restart_every: Duration,
    /// The agent whose call device may send us transcripts (C2). `None` accepts none.
    pub transcripts_from: Option<OwnedUserId>,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            delayed_leave_ms: DELAYED_LEAVE_MS,
            restart_every: RESTART_EVERY,
            transcripts_from: None,
        }
    }
}

/// What happens in a call.
#[derive(Debug, Clone, PartialEq)]
pub enum CallEvent {
    MemberJoined(CallMember),
    MemberLeft(CallMember),
    /// Our key went to this member's device.
    KeySent {
        to: CallMember,
    },
    /// A peer's key is registered for its LiveKit identity.
    KeyRegistered {
        identity: String,
    },
    KeyIgnored {
        reason: String,
    },
    Transcript {
        from: CallMember,
        content: TranscriptContent,
    },
    Media(MediaEvent),
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The homeserver and token-service half of joining: everything before media.
pub(crate) struct Negotiated {
    pub service_url: String,
    pub grant: LiveKitGrant,
    pub state_key: String,
    pub membership_id: String,
    pub delay_id: String,
    pub restart: RestartLoop,
}

/// Discovery → OpenID → token → identity check → arm delayed leave → membership → restart
/// loop → delayed-leave hand-off. Nothing is published unless the grant is good.
pub(crate) async fn negotiate(
    hs: &Homeserver,
    issuer: Option<Arc<dyn TokenIssuer>>,
    room_id: &RoomId,
    user_id: &UserId,
    device_id: &OwnedDeviceId,
    options: &CallOptions,
) -> Result<Negotiated, Error> {
    let service_url = hs.livekit_service_url().await?;
    let issuer = issuer.unwrap_or_else(|| Arc::new(SuperExchangeIssuer::new(&service_url)));
    let openid_token = hs.openid_token(user_id.as_str()).await?;
    let grant = issuer
        .token(&TokenRequest {
            room_id: room_id.to_string(),
            device_id: device_id.to_string(),
            openid_token: openid_token.clone(),
        })
        .await?;
    let expected = livekit_identity(user_id.as_str(), device_id.as_str());
    if grant.identity != expected {
        return Err(Error::IdentityMismatch {
            expected,
            got: grant.identity,
        });
    }
    let state_key = membership_state_key(user_id.as_str(), device_id.as_str());
    let delay_id = hs
        .arm_delayed_leave(room_id.as_str(), &state_key, options.delayed_leave_ms)
        .await?;
    let membership_id = uuid::Uuid::new_v4().to_string();
    let content = membership_content(device_id.as_str(), &membership_id, &service_url, room_id.as_str());
    if let Err(e) = hs.put_membership(room_id.as_str(), &state_key, &content).await {
        let _ = hs.delayed_action(&delay_id, DelayedAction::Cancel).await;
        return Err(e);
    }
    let restart = spawn_restart_loop(Arc::new(hs.clone()), delay_id.clone(), options.restart_every);
    let handoff = DelayedLeaveRequest {
        room_id: room_id.to_string(),
        device_id: device_id.to_string(),
        delay_id: delay_id.clone(),
        openid_token,
    };
    if let Err(e) = issuer.delayed_leave(&handoff).await {
        tracing::warn!("token service did not take the delayed leave: {e}");
    }
    Ok(Negotiated {
        service_url,
        grant,
        state_key,
        membership_id,
        delay_id,
        restart,
    })
}

/// What exists once we are in the call.
struct Joined {
    hs: Homeserver,
    grant: LiveKitGrant,
    service_url: String,
    state_key: String,
    membership_id: String,
    delay_id: String,
    media: Media,
}

struct Inner {
    client: Client,
    room_id: OwnedRoomId,
    user_id: OwnedUserId,
    device_id: OwnedDeviceId,
    options: CallOptions,
    own_key: CallKey,
    /// Peer keys go here from the start, so keys that arrive before media connects are kept.
    keys: KeyProvider,
    state: Mutex<CallState>,
    joined: OnceCell<Joined>,
    restart: StdMutex<Option<RestartLoop>>,
    left: AtomicBool,
    events: mpsc::UnboundedSender<CallEvent>,
}

/// One device's part in one room's call. Cheap to clone; all clones are the same call.
///
/// Create it with [`Call::new`], make it reachable from the sync loop (to-device events can
/// arrive as soon as our membership is published), then [`Call::join`].
#[derive(Clone)]
pub struct Call {
    inner: Arc<Inner>,
}

impl Call {
    /// A call in `room_id` that has not been joined yet. No network.
    pub fn new(
        client: &Client,
        room_id: &RoomId,
        options: CallOptions,
    ) -> Result<(Call, mpsc::UnboundedReceiver<CallEvent>), Error> {
        let user_id = client.user_id().ok_or(Error::NotLoggedIn)?.to_owned();
        let device_id = client.device_id().ok_or(Error::NotLoggedIn)?.to_owned();
        let (tx, rx) = mpsc::unbounded_channel();
        let state = CallState::new(
            room_id.to_owned(),
            user_id.clone(),
            device_id.clone(),
            options.transcripts_from.clone(),
        );
        let inner = Arc::new(Inner {
            client: client.clone(),
            room_id: room_id.to_owned(),
            user_id,
            device_id,
            options,
            own_key: CallKey::generate(),
            keys: new_key_provider(),
            state: Mutex::new(state),
            joined: OnceCell::new(),
            restart: StdMutex::new(None),
            left: AtomicBool::new(false),
            events: tx,
        });
        Ok((Call { inner }, rx))
    }

    /// Joins the call. `issuer` defaults to SuperExchange's token service at the homeserver's
    /// discovered `livekit_service_url`. Joining twice is an error.
    pub async fn join(&self, issuer: Option<Arc<dyn TokenIssuer>>) -> Result<(), Error> {
        let i = &self.inner;
        if i.joined.initialized() {
            return Err(Error::Matrix("already joined".into()));
        }
        if i.client.get_room(&i.room_id).is_none() {
            return Err(Error::UnknownRoom(i.room_id.to_string()));
        }
        let hs = Homeserver::from_client(&i.client)?;
        let n = negotiate(&hs, issuer, &i.room_id, &i.user_id, &i.device_id, &i.options).await?;
        let (media, mut media_rx) = match Media::connect(&n.grant, i.keys.clone(), &i.own_key).await {
            Ok(m) => m,
            Err(e) => {
                n.restart.stop().await;
                let _ = hs
                    .clear_membership(i.room_id.as_str(), &n.state_key, Some(&n.delay_id))
                    .await;
                return Err(e);
            }
        };
        let tx = i.events.clone();
        tokio::spawn(async move {
            while let Some(ev) = media_rx.recv().await {
                if tx.send(CallEvent::Media(ev)).is_err() {
                    break;
                }
            }
        });
        *i.restart.lock().expect("restart lock") = Some(n.restart);
        let joined = Joined {
            hs,
            grant: n.grant,
            service_url: n.service_url,
            state_key: n.state_key,
            membership_id: n.membership_id,
            delay_id: n.delay_id,
            media,
        };
        if i.joined.set(joined).is_err() {
            return Err(Error::Matrix("joined concurrently".into()));
        }
        self.refresh_members().await
    }

    fn joined(&self) -> Option<&Joined> {
        self.inner.joined.get()
    }

    pub fn room_id(&self) -> &RoomId {
        &self.inner.room_id
    }

    /// The LiveKit grant, once joined.
    pub fn grant(&self) -> Option<&LiveKitGrant> {
        self.joined().map(|j| &j.grant)
    }

    /// The LiveKit connection, once joined.
    pub fn media(&self) -> Option<&Media> {
        self.joined().map(|j| &j.media)
    }

    /// The delayed leave's ID, once joined.
    pub fn delay_id(&self) -> Option<&str> {
        self.joined().map(|j| j.delay_id.as_str())
    }

    pub fn membership_id(&self) -> Option<&str> {
        self.joined().map(|j| j.membership_id.as_str())
    }

    pub fn livekit_service_url(&self) -> Option<&str> {
        self.joined().map(|j| j.service_url.as_str())
    }

    /// The other participants' devices, as last refreshed.
    pub async fn members(&self) -> Vec<CallMember> {
        self.inner.state.lock().await.members().to_vec()
    }

    fn emit(&self, ev: CallEvent) {
        let _ = self.inner.events.send(ev);
    }

    /// Re-reads the room's call memberships from the client's store (call it after each sync):
    /// reports joins and leaves, registers keys that were waiting for their sender's
    /// membership, and, once we are in the call, sends our key to new members.
    pub async fn refresh_members(&self) -> Result<(), Error> {
        let i = &self.inner;
        let Some(room) = i.client.get_room(&i.room_id) else {
            return Ok(());
        };
        let raw = room
            .get_state_events(StateEventType::from(CALL_MEMBER_EVENT))
            .await
            .map_err(|e| Error::Matrix(e.to_string()))?;
        let events: Vec<Value> = raw
            .into_iter()
            .filter_map(|r| match r {
                RawAnySyncOrStrippedState::Sync(r) => r.deserialize_as::<Value>().ok(),
                _ => None,
            })
            .collect();
        let mut active = Vec::new();
        for m in active_members(&events, now_ms()) {
            let joined = room
                .get_member_no_sync(&m.user_id)
                .await
                .ok()
                .flatten()
                .is_some_and(|rm| *rm.membership() == MembershipState::Join);
            if joined {
                active.push(m);
            }
        }
        self.apply_members(active).await;
        Ok(())
    }

    pub(crate) async fn apply_members(&self, active: Vec<CallMember>) {
        let i = &self.inner;
        let in_call = i.joined.get().is_some() && !i.left.load(Ordering::SeqCst);
        let diff = i.state.lock().await.update_members(active);
        for m in diff.left {
            self.emit(CallEvent::MemberLeft(m));
        }
        for m in diff.joined {
            self.emit(CallEvent::MemberJoined(m));
        }
        for k in diff.keys_to_apply {
            self.register_key(&k.identity, k.index, &k.key);
        }
        for m in diff.to_key {
            let sent = match i.joined.get() {
                Some(j) if in_call => self.send_own_key(&m, &j.membership_id).await,
                _ => Err(Error::Matrix("not in the call yet".into())),
            };
            match sent {
                Ok(()) => self.emit(CallEvent::KeySent { to: m }),
                Err(e) => {
                    if in_call {
                        tracing::warn!(to = %m.identity(), "could not send our call key yet: {e}");
                    }
                    i.state.lock().await.mark_unkeyed(&m);
                }
            }
        }
    }

    fn register_key(&self, identity: &str, index: u32, key: &CallKey) {
        let index = i32::try_from(index).unwrap_or(i32::MAX);
        self.inner.keys.set_key(
            &ParticipantIdentity(identity.to_owned()),
            index,
            key.as_bytes().to_vec(),
        );
        self.emit(CallEvent::KeyRegistered {
            identity: identity.to_owned(),
        });
    }

    async fn send_own_key(&self, to: &CallMember, membership_id: &str) -> Result<(), Error> {
        let i = &self.inner;
        let content = key_message(
            i.room_id.as_str(),
            i.user_id.as_str(),
            i.device_id.as_str(),
            membership_id,
            0,
            &i.own_key,
            now_ms(),
        );
        let value = serde_json::to_value(content).map_err(|e| Error::Malformed(e.to_string()))?;
        self.send_to_device(to, KEYS_EVENT, &value).await
    }

    async fn send_to_device(&self, to: &CallMember, event_type: &str, content: &Value) -> Result<(), Error> {
        let enc = self.inner.client.encryption();
        let mut device = enc
            .get_device(&to.user_id, &to.device_id)
            .await
            .map_err(|e| Error::Matrix(e.to_string()))?;
        if device.is_none() {
            let _ = enc.request_user_identity(&to.user_id).await;
            device = enc
                .get_device(&to.user_id, &to.device_id)
                .await
                .map_err(|e| Error::Matrix(e.to_string()))?;
        }
        let device = device.ok_or_else(|| Error::Matrix(format!("device {} not known", to.identity())))?;
        let raw: Raw<AnyToDeviceEventContent> = Raw::new(content)
            .map_err(|e| Error::Malformed(e.to_string()))?
            .cast_unchecked();
        let failed = enc
            .encrypt_and_send_raw_to_device(vec![&device], event_type, raw, CollectStrategy::AllDevices)
            .await
            .map_err(|e| Error::Matrix(e.to_string()))?;
        if failed.is_empty() {
            Ok(())
        } else {
            Err(Error::Matrix(format!(
                "to-device {event_type} to {} failed",
                to.identity()
            )))
        }
    }

    /// Takes one to-device event from sync. C1 keys and C2 transcripts for this room are
    /// handled; cleartext ones are ignored, and everything else is left alone. Works before
    /// [`Call::join`] returns.
    pub async fn handle_to_device(&self, ev: &ProcessedToDeviceEvent) {
        let (raw, sender) = match ev {
            ProcessedToDeviceEvent::Decrypted { raw, encryption_info } => (
                raw,
                Some((encryption_info.sender.clone(), encryption_info.sender_device.clone())),
            ),
            ProcessedToDeviceEvent::PlainText(raw) => (raw, None),
            _ => return,
        };
        let Ok(v) = raw.deserialize_as::<Value>() else { return };
        let ty = v["type"].as_str().unwrap_or_default();
        if ty != KEYS_EVENT && ty != TRANSCRIPT_EVENT {
            return;
        }
        let content = &v["content"];
        if content["room_id"].as_str() != Some(self.inner.room_id.as_str()) {
            return;
        }
        let Some((sender, Some(sender_device))) = sender else {
            if ty == KEYS_EVENT {
                self.emit(CallEvent::KeyIgnored {
                    reason: KeyRejection::Unencrypted.to_string(),
                });
            } else {
                tracing::warn!("transcript ignored: {}", TranscriptRejection::Unencrypted);
            }
            return;
        };
        if ty == KEYS_EVENT {
            match parse_key_event(content, &self.inner.room_id, &sender, &sender_device) {
                Ok(k) => {
                    let now = self.inner.state.lock().await.on_key(k);
                    if let Some(k) = now {
                        self.register_key(&k.identity, k.index, &k.key);
                    }
                }
                Err(e) => self.emit(CallEvent::KeyIgnored { reason: e.to_string() }),
            }
            return;
        }
        let parsed: Result<TranscriptContent, _> = serde_json::from_value(content.clone());
        let result = match parsed {
            Ok(t) => self
                .inner
                .state
                .lock()
                .await
                .accept_transcript(&t, &sender, &sender_device)
                .map(|from| (from, t)),
            Err(e) => Err(TranscriptRejection::Malformed(e.to_string())),
        };
        match result {
            Ok((from, content)) => self.emit(CallEvent::Transcript { from, content }),
            Err(e) => tracing::warn!(%sender, %sender_device, "transcript ignored: {e}"),
        }
    }

    /// Sends a C2 transcript to a member's call device.
    pub async fn send_transcript(&self, to: &CallMember, content: &TranscriptContent) -> Result<(), Error> {
        if !self.inner.state.lock().await.is_member(&to.user_id, &to.device_id) {
            return Err(Error::Matrix(format!("{} is not in the call", to.identity())));
        }
        let value = serde_json::to_value(content).map_err(|e| Error::Malformed(e.to_string()))?;
        self.send_to_device(to, TRANSCRIPT_EVENT, &value).await
    }

    /// Sends 48 kHz mono PCM as our microphone.
    pub async fn capture(&self, samples: &[i16]) -> Result<(), Error> {
        match self.media() {
            Some(m) => m.capture(samples).await,
            None => Err(Error::LiveKit("not connected".into())),
        }
    }

    /// Leaves: stops the restart loop, closes media and clears our membership (sending the
    /// delayed leave now, or publishing the empty membership). Safe to call more than once.
    pub async fn leave(&self) -> Result<(), Error> {
        if self.inner.left.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let restart = self.inner.restart.lock().expect("restart lock").take();
        if let Some(r) = restart {
            r.stop().await;
        }
        let Some(j) = self.joined() else { return Ok(()) };
        j.media.close().await;
        j.hs.clear_membership(self.inner.room_id.as_str(), &j.state_key, Some(&j.delay_id))
            .await
    }
}

/// C3: posts a transcript as our own message in the room, marked as spoken in a call.
pub async fn post_spoken_turn(
    room: &matrix_sdk::Room,
    content: &TranscriptContent,
    agent_user_id: &UserId,
) -> Result<OwnedEventId, Error> {
    let marker = SpokenMarker {
        turn_id: content.turn_id.clone(),
        agent_user_id: agent_user_id.to_string(),
        audio_ms: content.audio_ms,
    };
    let body = spoken_message_content(&content.text, &marker);
    let r = room
        .send_raw("m.room.message", body)
        .await
        .map_err(|e| Error::Matrix(e.to_string()))?;
    Ok(r.response.event_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::issuer::{IssuerError, RefusalReason};
    use serde_json::json;
    use std::sync::Mutex;
    use wiremock::{
        matchers::{method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    struct FakeIssuer {
        identity: String,
        refuse: bool,
        handed: Mutex<Vec<DelayedLeaveRequest>>,
    }

    #[async_trait::async_trait]
    impl TokenIssuer for FakeIssuer {
        async fn token(&self, _req: &TokenRequest) -> Result<LiveKitGrant, IssuerError> {
            if self.refuse {
                return Err(IssuerError::Refused(RefusalReason::HarnessMode));
            }
            Ok(LiveKitGrant {
                url: "wss://lk".into(),
                jwt: "j".into(),
                identity: self.identity.clone(),
                livekit_room: "room".into(),
            })
        }
        async fn delayed_leave(&self, req: &DelayedLeaveRequest) -> Result<(), IssuerError> {
            self.handed.lock().unwrap().push(req.clone());
            Ok(())
        }
    }

    fn issuer(identity: &str, refuse: bool) -> Arc<FakeIssuer> {
        Arc::new(FakeIssuer {
            identity: identity.into(),
            refuse,
            handed: Mutex::new(vec![]),
        })
    }

    const MEMBER_PATH: &str =
        "/_matrix/client/v3/rooms/%21r%3As/state/org.matrix.msc3401.call.member/_%40u%3As_DEV_m.call";

    async fn homeserver() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                json!({"rtc_transports": [{"type": "livekit", "livekit_service_url": "https://lk.example"}]}),
            ))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_matrix/client/v3/user/%40u%3As/openid/request_token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"access_token": "o"})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(MEMBER_PATH))
            .and(query_param("org.matrix.msc4140.delay", "8000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"delay_id": "d1"})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(MEMBER_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id": "$m"})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4140/delayed_events/d1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount(&server)
            .await;
        server
    }

    fn who() -> (OwnedRoomId, OwnedUserId, OwnedDeviceId) {
        ("!r:s".try_into().unwrap(), "@u:s".try_into().unwrap(), "DEV".into())
    }

    async fn puts(server: &MockServer) -> Vec<(bool, Value)> {
        server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.method.as_str() == "PUT")
            .map(|r| {
                let delayed = r
                    .url
                    .query()
                    .is_some_and(|q| q.contains("org.matrix.msc4140.delay=8000"));
                (delayed, serde_json::from_slice(&r.body).unwrap())
            })
            .collect()
    }

    #[tokio::test]
    async fn negotiate_arms_delay_before_membership_and_hands_off_delay_id() {
        let server = homeserver().await;
        let hs = Homeserver::new(&server.uri(), "tok");
        let iss = issuer("@u:s:DEV", false);
        let (room, user, device) = who();
        let n = negotiate(&hs, Some(iss.clone()), &room, &user, &device, &CallOptions::default())
            .await
            .unwrap();
        assert_eq!(n.delay_id, "d1");
        assert_eq!(n.service_url, "https://lk.example");
        assert_eq!(n.state_key, "_@u:s_DEV_m.call");
        let p = puts(&server).await;
        assert_eq!(p.len(), 2);
        assert!(p[0].0, "first PUT arms the delayed leave");
        assert_eq!(p[0].1, json!({}));
        assert!(!p[1].0);
        assert_eq!(
            p[1].1,
            membership_content("DEV", &n.membership_id, "https://lk.example", "!r:s")
        );
        let handed = iss.handed.lock().unwrap().clone();
        assert_eq!(handed.len(), 1);
        assert_eq!(handed[0].delay_id, "d1");
        assert_eq!(handed[0].room_id, "!r:s");
        assert_eq!(handed[0].device_id, "DEV");
        n.restart.stop().await;
    }

    #[tokio::test]
    async fn negotiate_refuses_identity_mismatch() {
        let server = homeserver().await;
        let hs = Homeserver::new(&server.uri(), "tok");
        let (room, user, device) = who();
        let r = negotiate(
            &hs,
            Some(issuer("@u:s:OTHER", false)),
            &room,
            &user,
            &device,
            &CallOptions::default(),
        )
        .await;
        match r {
            Err(Error::IdentityMismatch { expected, got }) => {
                assert_eq!(expected, "@u:s:DEV");
                assert_eq!(got, "@u:s:OTHER");
            }
            Err(e) => panic!("unexpected error {e}"),
            Ok(_) => panic!("unexpected success"),
        }
        assert!(puts(&server).await.is_empty());
    }

    #[tokio::test]
    async fn negotiate_refusal_publishes_nothing() {
        let server = homeserver().await;
        let hs = Homeserver::new(&server.uri(), "tok");
        let (room, user, device) = who();
        let r = negotiate(
            &hs,
            Some(issuer("@u:s:DEV", true)),
            &room,
            &user,
            &device,
            &CallOptions::default(),
        )
        .await;
        assert!(matches!(
            r,
            Err(Error::Issuer(IssuerError::Refused(RefusalReason::HarnessMode)))
        ));
        assert!(puts(&server).await.is_empty());
    }

    async fn logged_in_client(server: &MockServer) -> Client {
        use matrix_sdk::{
            authentication::matrix::MatrixSession,
            ruma::{owned_device_id, owned_user_id},
            SessionMeta, SessionTokens,
        };
        let client = Client::builder().homeserver_url(server.uri()).build().await.unwrap();
        client
            .restore_session(MatrixSession {
                meta: SessionMeta {
                    user_id: owned_user_id!("@u:s"),
                    device_id: owned_device_id!("DEV"),
                },
                tokens: SessionTokens {
                    access_token: "tok".into(),
                    refresh_token: None,
                },
            })
            .await
            .unwrap();
        client
    }

    fn decrypted_key_event(sender: &str, device: &str, key: &CallKey) -> ProcessedToDeviceEvent {
        use matrix_sdk::deserialized_responses::{AlgorithmInfo, EncryptionInfo, VerificationLevel, VerificationState};
        let content = key_message("!r:s", sender, device, "m-peer", 0, key, 1);
        let raw = Raw::new(&json!({"type": KEYS_EVENT, "sender": sender, "content": content}))
            .unwrap()
            .cast_unchecked();
        ProcessedToDeviceEvent::Decrypted {
            raw,
            encryption_info: EncryptionInfo {
                sender: sender.try_into().unwrap(),
                sender_device: Some(device.into()),
                forwarder: None,
                algorithm_info: AlgorithmInfo::OlmV1Curve25519AesSha2 {
                    curve25519_public_key_base64: "k".into(),
                },
                verification_state: VerificationState::Unverified(VerificationLevel::UnsignedDevice),
            },
        }
    }

    #[tokio::test]
    async fn key_received_before_join_is_registered_and_peer_keyed_after_join() {
        // A peer's key can arrive between our membership being published and join() returning,
        // so a call must take to-device events from the moment it exists.
        let server = MockServer::start().await;
        let client = logged_in_client(&server).await;
        let room: OwnedRoomId = "!r:s".try_into().unwrap();
        let (call, mut rx) = Call::new(&client, &room, CallOptions::default()).unwrap();
        let key = CallKey::generate();
        call.handle_to_device(&decrypted_key_event("@peer:s", "PEER", &key))
            .await;
        let peer = CallMember {
            user_id: "@peer:s".try_into().unwrap(),
            device_id: "PEER".into(),
            membership_id: "m-peer".into(),
        };
        call.apply_members(vec![peer.clone()]).await;
        assert_eq!(
            call.inner
                .keys
                .get_key(&livekit::prelude::ParticipantIdentity("@peer:s:PEER".into()), 0),
            Some(key.as_bytes().to_vec())
        );
        let mut seen = vec![];
        while let Ok(ev) = rx.try_recv() {
            seen.push(ev);
        }
        assert!(seen.contains(&CallEvent::MemberJoined(peer.clone())));
        assert!(seen.contains(&CallEvent::KeyRegistered {
            identity: "@peer:s:PEER".into()
        }));
        assert!(
            !seen.iter().any(|e| matches!(e, CallEvent::KeySent { .. })),
            "no key sent before we are in the call"
        );
        // Not joined yet, so the peer is still owed our key.
        assert_eq!(
            call.inner.state.lock().await.update_members(vec![peer.clone()]).to_key,
            vec![peer]
        );
    }

    fn assert_send<T: Send>(_: T) {}

    #[tokio::test]
    async fn call_futures_are_send() {
        // superdial-agent and SuperMessage spawn these on multi-threaded runtimes.
        let server = MockServer::start().await;
        let client = logged_in_client(&server).await;
        let room: OwnedRoomId = "!r:s".try_into().unwrap();
        let (call, _rx) = Call::new(&client, &room, CallOptions::default()).unwrap_or_else(|_| unreachable!());
        assert_send(call.join(None));
    }

    #[test]
    fn default_options() {
        let o = CallOptions::default();
        assert_eq!(o.delayed_leave_ms, 8000);
        assert_eq!(o.restart_every, Duration::from_secs(5));
        assert_eq!(o.transcripts_from, None);
    }
}
