//! C6, the hub → superdial-agent call API (treaty `schemas/http/superdial/agent-calls.v1`):
//! join, leave, a spoken turn's reply, health. Bearer `SUPERDIAL_AGENT_TOKEN` on every route.
//! The handlers drive a [`CallLayer`]; the real one is `agent::Agent`.

use std::{
    fmt,
    sync::{Arc, LazyLock},
};

use async_trait::async_trait;
use axum::{
    body::Bytes,
    extract::{Path, Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use matrix_sdk::ruma::{OwnedRoomId, OwnedUserId, RoomId};
use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::json;

/// The agent's call device, as the hub minted it.
#[derive(Clone, Deserialize, PartialEq, Eq)]
pub struct DeviceCredentials {
    pub device_id: String,
    pub access_token: String,
}

impl fmt::Debug for DeviceCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeviceCredentials")
            .field("device_id", &self.device_id)
            .field("access_token", &"<redacted>")
            .finish()
    }
}

/// `PUT /v1/calls/{room_id}`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct JoinRequest {
    pub agent_user_id: OwnedUserId,
    pub homeserver_url: String,
    pub device: DeviceCredentials,
    pub voice: String,
    /// Required, and null when the speech service should detect the language.
    #[serde(deserialize_with = "Option::deserialize")]
    pub stt_language: Option<String>,
}

/// How a spoken turn ended.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Ok,
    Cancelled,
    Error,
}

/// `POST /v1/calls/{room_id}/turns/{turn_id}/reply`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Reply {
    pub seq: u64,
    pub text: String,
    #[serde(rename = "final")]
    pub is_final: bool,
    pub outcome: Outcome,
}

/// Why a request was not done.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, errcode) = match &self {
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, "M_BAD_JSON"),
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, "M_NOT_FOUND"),
            ApiError::Conflict(_) => (StatusCode::CONFLICT, "M_UNKNOWN"),
            ApiError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "M_UNKNOWN"),
        };
        (status, Json(json!({"errcode": errcode, "error": self.to_string()}))).into_response()
    }
}

/// What the handlers drive.
#[async_trait]
pub trait CallLayer: Send + Sync + 'static {
    /// Join `room`'s call as `req.agent_user_id`, or stay in it. Returns once accepted; the join
    /// itself goes on in the background.
    async fn join(&self, room: OwnedRoomId, req: JoinRequest) -> Result<(), ApiError>;
    /// Leave `room`'s call, cancelling any turn waiting. A room with no call is fine.
    async fn leave(&self, room: &RoomId);
    /// A spoken turn's reply. `NotFound` when there is no such call or turn.
    async fn reply(&self, room: &RoomId, turn_id: &str, reply: Reply) -> Result<(), ApiError>;
    /// How many calls are running.
    fn calls(&self) -> usize;
}

/// The C6 router.
pub fn router(layer: Arc<dyn CallLayer>, token: String) -> Router {
    Router::new()
        .route("/v1/calls/{room_id}", put(join).delete(leave))
        .route("/v1/calls/{room_id}/turns/{turn_id}/reply", post(reply))
        .route("/v1/health", get(health))
        .layer(middleware::from_fn_with_state(Arc::new(token), require_bearer))
        .with_state(layer)
}

type Layer = State<Arc<dyn CallLayer>>;

async fn require_bearer(State(token): State<Arc<String>>, req: Request, next: Next) -> Response {
    let given = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    match given {
        Some(t) if constant_time_eq(t.as_bytes(), token.as_bytes()) => next.run(req).await,
        Some(_) => unauthorized("M_UNKNOWN_TOKEN", "bad token"),
        None => unauthorized("M_MISSING_TOKEN", "missing bearer token"),
    }
}

fn unauthorized(errcode: &str, error: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({"errcode": errcode, "error": error})),
    )
        .into_response()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn room_id(raw: &str) -> Result<OwnedRoomId, ApiError> {
    RoomId::parse(raw).map_err(|_| ApiError::BadRequest(format!("{raw:?} is not a room id")))
}

fn body<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, ApiError> {
    serde_json::from_slice(bytes).map_err(|e| ApiError::BadRequest(format!("invalid body: {e}")))
}

fn turn_id(raw: &str) -> Result<&str, ApiError> {
    static TURN: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$").unwrap());
    if TURN.is_match(raw) {
        Ok(raw)
    } else {
        Err(ApiError::BadRequest(format!("{raw:?} is not a lower-case UUIDv4")))
    }
}

impl JoinRequest {
    fn validate(&self) -> Result<(), ApiError> {
        let bad = |why: &str| Err(ApiError::BadRequest(why.into()));
        if !(self.homeserver_url.starts_with("https://") || self.homeserver_url.starts_with("http://")) {
            return bad("homeserver_url must be an http(s) URL");
        }
        if self.device.device_id.is_empty() || self.device.access_token.is_empty() {
            return bad("device needs a device_id and an access_token");
        }
        if self.voice.is_empty() {
            return bad("voice is empty");
        }
        Ok(())
    }
}

async fn join(State(layer): Layer, Path(room): Path<String>, bytes: Bytes) -> Result<StatusCode, ApiError> {
    let room = room_id(&room)?;
    let req: JoinRequest = body(&bytes)?;
    req.validate()?;
    tracing::info!(room = %room, agent = %req.agent_user_id, device = %req.device.device_id, "join requested");
    layer.join(room, req).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn leave(State(layer): Layer, Path(room): Path<String>) -> Result<StatusCode, ApiError> {
    let room = room_id(&room)?;
    tracing::info!(room = %room, "leave requested");
    layer.leave(&room).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn reply(
    State(layer): Layer,
    Path((room, turn)): Path<(String, String)>,
    bytes: Bytes,
) -> Result<StatusCode, ApiError> {
    let room = room_id(&room)?;
    let turn = turn_id(&turn)?;
    let reply: Reply = body(&bytes)?;
    tracing::info!(room = %room, turn_id = turn, seq = reply.seq, r#final = reply.is_final, outcome = ?reply.outcome, chars = reply.text.chars().count(), "reply");
    layer.reply(&room, turn, reply).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn health(State(layer): Layer) -> Json<serde_json::Value> {
    Json(json!({"calls": layer.calls()}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::sync::Mutex;
    use tower::ServiceExt;

    const TOKEN: &str = "loopback-secret";
    const TURN: &str = "0b6c7c4e-3f0e-4c1b-9a43-6f1d2a9e5b77";

    #[derive(Default)]
    struct Fake {
        log: Mutex<Vec<String>>,
        joins: Mutex<Vec<(OwnedRoomId, JoinRequest)>>,
        replies: Mutex<Vec<(String, String, Reply)>>,
    }

    #[async_trait]
    impl CallLayer for Fake {
        async fn join(&self, room: OwnedRoomId, req: JoinRequest) -> Result<(), ApiError> {
            self.log.lock().unwrap().push(format!("join {room}"));
            self.joins.lock().unwrap().push((room, req));
            Ok(())
        }
        async fn leave(&self, room: &RoomId) {
            self.log.lock().unwrap().push(format!("leave {room}"));
        }
        async fn reply(&self, room: &RoomId, turn_id: &str, reply: Reply) -> Result<(), ApiError> {
            if turn_id == "00000000-0000-4000-8000-000000000000" {
                return Err(ApiError::NotFound("no such turn".into()));
            }
            self.log.lock().unwrap().push(format!("reply {room} {turn_id}"));
            self.replies
                .lock()
                .unwrap()
                .push((room.to_string(), turn_id.to_owned(), reply));
            Ok(())
        }
        fn calls(&self) -> usize {
            2
        }
    }

    type Cases = Vec<(String, Value)>;

    /// The treaty fixture's (valid, invalid) request bodies for an operation.
    fn cases(op: &str) -> (Cases, Cases) {
        let path = format!(
            "{}/tests/fixtures/agent-calls.v1.cases.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let all: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let req = &all["operations"][op]["request"];
        let valid = req["valid"]
            .as_object()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let invalid = req["invalid"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| (c["why"].as_str().unwrap().to_owned(), c["body"].clone()))
            .collect();
        (valid, invalid)
    }

    async fn send(
        fake: &Arc<Fake>,
        method: &str,
        uri: &str,
        token: Option<&str>,
        body: Option<&Value>,
    ) -> (StatusCode, Value) {
        let app = router(fake.clone(), TOKEN.into());
        let mut req = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            req = req.header("authorization", format!("Bearer {t}"));
        }
        let req = match body {
            Some(b) => req
                .header("content-type", "application/json")
                .body(Body::from(b.to_string())),
            None => req.body(Body::empty()),
        }
        .unwrap();
        let res = app.oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, v)
    }

    fn reply_uri(turn: &str) -> String {
        format!("/v1/calls/%21r%3As/turns/{turn}/reply")
    }

    #[tokio::test]
    async fn missing_or_wrong_bearer_is_401() {
        let fake = Arc::new(Fake::default());
        let join = cases("joinSuperdialCall").0[0].1.clone();
        let reply = json!({"seq": 0, "text": "x", "final": true, "outcome": "ok"});
        for token in [None, Some("wrong"), Some("loopback-secre")] {
            for (m, uri, body) in [
                ("PUT", "/v1/calls/%21r%3As".to_owned(), Some(&join)),
                ("DELETE", "/v1/calls/%21r%3As".to_owned(), None),
                ("POST", reply_uri(TURN), Some(&reply)),
                ("GET", "/v1/health".to_owned(), None),
            ] {
                let (status, body) = send(&fake, m, &uri, token, body).await;
                assert_eq!(status, StatusCode::UNAUTHORIZED, "{m} {uri} {token:?}");
                assert!(body["errcode"].is_string());
            }
        }
        assert!(fake.log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn put_valid_fixture_cases_are_202() {
        let fake = Arc::new(Fake::default());
        for (why, body) in cases("joinSuperdialCall").0 {
            let (status, _) = send(&fake, "PUT", "/v1/calls/%21r%3As", Some(TOKEN), Some(&body)).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{why}");
        }
        let joins = fake.joins.lock().unwrap();
        assert_eq!(joins.len(), 2);
        assert_eq!(joins[0].0.as_str(), "!r:s");
        let languages: Vec<_> = joins.iter().map(|(_, r)| r.stt_language.clone()).collect();
        assert!(languages.contains(&Some("en".into())) && languages.contains(&None));
        assert_eq!(joins[0].1.device.device_id, "SUPERDIAL");
        assert_eq!(joins[0].1.voice, "af_heart");
        assert_eq!(joins[0].1.agent_user_id.as_str(), "@agent_hermes:example.org");
    }

    #[tokio::test]
    async fn put_invalid_fixture_cases_are_400() {
        let fake = Arc::new(Fake::default());
        for (why, body) in cases("joinSuperdialCall").1 {
            let (status, b) = send(&fake, "PUT", "/v1/calls/%21r%3As", Some(TOKEN), Some(&body)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{why}: {b}");
        }
        let mut empty_voice = cases("joinSuperdialCall").0[0].1.clone();
        empty_voice["voice"] = json!("");
        let (status, _) = send(&fake, "PUT", "/v1/calls/%21r%3As", Some(TOKEN), Some(&empty_voice)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(fake.log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn put_bad_room_id_is_400() {
        let fake = Arc::new(Fake::default());
        let body = cases("joinSuperdialCall").0[0].1.clone();
        let (status, _) = send(&fake, "PUT", "/v1/calls/not-a-room", Some(TOKEN), Some(&body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(fake.log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_is_204_even_without_call() {
        let fake = Arc::new(Fake::default());
        let (status, body) = send(&fake, "DELETE", "/v1/calls/%21r%3As", Some(TOKEN), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(body, Value::Null);
        assert_eq!(*fake.log.lock().unwrap(), vec!["leave !r:s".to_owned()]);
    }

    #[tokio::test]
    async fn reply_valid_fixture_cases_are_204() {
        let fake = Arc::new(Fake::default());
        for (why, body) in cases("replySuperdialTurn").0 {
            let (status, _) = send(&fake, "POST", &reply_uri(TURN), Some(TOKEN), Some(&body)).await;
            assert_eq!(status, StatusCode::NO_CONTENT, "{why}");
        }
        let replies = fake.replies.lock().unwrap();
        assert_eq!(replies.len(), 4);
        assert!(replies.iter().all(|(room, turn, _)| room == "!r:s" && turn == TURN));
        assert!(replies.iter().any(|(_, _, r)| r
            == &Reply {
                seq: 0,
                text: "The build is green.".into(),
                is_final: true,
                outcome: Outcome::Ok
            }));
        assert!(replies.iter().any(|(_, _, r)| r.outcome == Outcome::Cancelled));
        assert!(replies.iter().any(|(_, _, r)| r.outcome == Outcome::Error));
        assert!(replies.iter().any(|(_, _, r)| !r.is_final && r.seq == 1));
    }

    #[tokio::test]
    async fn reply_invalid_fixture_cases_are_400() {
        let fake = Arc::new(Fake::default());
        for (why, body) in cases("replySuperdialTurn").1 {
            let (status, _) = send(&fake, "POST", &reply_uri(TURN), Some(TOKEN), Some(&body)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{why}");
        }
        assert!(fake.log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reply_turn_id_must_be_lowercase_uuid_v4() {
        let fake = Arc::new(Fake::default());
        let body = json!({"seq": 0, "text": "x", "final": true, "outcome": "ok"});
        for bad in [
            "0B6C7C4E-3F0E-4C1B-9A43-6F1D2A9E5B77",
            "0b6c7c4e-3f0e-1c1b-9a43-6f1d2a9e5b77",
            "not-a-turn",
        ] {
            let (status, _) = send(&fake, "POST", &reply_uri(bad), Some(TOKEN), Some(&body)).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
        }
        assert!(fake.log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn reply_unknown_turn_is_404() {
        let fake = Arc::new(Fake::default());
        let body = json!({"seq": 0, "text": "x", "final": true, "outcome": "ok"});
        let (status, b) = send(
            &fake,
            "POST",
            &reply_uri("00000000-0000-4000-8000-000000000000"),
            Some(TOKEN),
            Some(&body),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(b["errcode"], "M_NOT_FOUND");
    }

    #[tokio::test]
    async fn health_reports_calls() {
        let fake = Arc::new(Fake::default());
        let (status, body) = send(&fake, "GET", "/v1/health", Some(TOKEN), None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, json!({"calls": 2}));
    }

    #[test]
    fn join_request_debug_redacts_token() {
        let body = cases("joinSuperdialCall").0[0].1.clone();
        let req: JoinRequest = serde_json::from_value(body).unwrap();
        assert!(!format!("{req:?}").contains("syt_example_token"));
    }
}
