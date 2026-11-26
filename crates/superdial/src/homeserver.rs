//! The client-server API calls a call needs that matrix-sdk does not offer directly: MSC4143
//! transport discovery, OpenID tokens, MSC4140 delayed state and membership state.

use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};

use crate::{contract::CALL_MEMBER_EVENT, Error};

/// An action on an MSC4140 delayed event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayedAction {
    Restart,
    Send,
    Cancel,
}

impl DelayedAction {
    fn as_str(self) -> &'static str {
        match self {
            DelayedAction::Restart => "restart",
            DelayedAction::Send => "send",
            DelayedAction::Cancel => "cancel",
        }
    }
}

/// Authenticated raw access to one homeserver.
#[derive(Debug, Clone)]
pub struct Homeserver {
    base: String,
    access_token: String,
    http: reqwest::Client,
}

/// Percent-encodes one URL path segment, keeping only unreserved characters.
pub(crate) fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Homeserver {
    pub fn new(base_url: &str, access_token: &str) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("reqwest client");
        Self {
            base: base_url.trim_end_matches('/').to_owned(),
            access_token: access_token.to_owned(),
            http,
        }
    }

    /// The homeserver and access token of a logged-in matrix-sdk client.
    pub fn from_client(client: &matrix_sdk::Client) -> Result<Self, Error> {
        let token = client.access_token().ok_or(Error::NotLoggedIn)?;
        Ok(Self::new(client.homeserver().as_str(), &token))
    }

    async fn request(&self, method: Method, path: &str, body: Option<&Value>) -> Result<Value, Error> {
        let mut rb = self
            .http
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.access_token);
        if let Some(b) = body {
            rb = rb.json(b);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            let what = path.split('?').next().unwrap_or(path).to_owned();
            return Err(Error::Http {
                what,
                status: status.as_u16(),
                body: text,
            });
        }
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text).map_err(|e| Error::Malformed(format!("{path}: {e}")))
    }

    /// The first `livekit` transport's `livekit_service_url` (MSC4143), without a trailing `/`.
    /// This is the token service's base URL.
    pub async fn livekit_service_url(&self) -> Result<String, Error> {
        let v = self
            .request(
                Method::GET,
                "/_matrix/client/unstable/org.matrix.msc4143/rtc/transports",
                None,
            )
            .await?;
        v["rtc_transports"]
            .as_array()
            .and_then(|a| a.iter().find(|t| t["type"] == "livekit"))
            .and_then(|t| t["livekit_service_url"].as_str())
            .map(|u| u.trim_end_matches('/').to_owned())
            .ok_or(Error::NoLiveKitTransport)
    }

    /// A fresh OpenID token for `user_id`, exactly as the homeserver returned it.
    pub async fn openid_token(&self, user_id: &str) -> Result<Value, Error> {
        let path = format!(
            "/_matrix/client/v3/user/{}/openid/request_token",
            encode_segment(user_id)
        );
        self.request(Method::POST, &path, Some(&json!({}))).await
    }

    fn member_path(room_id: &str, state_key: &str) -> String {
        format!(
            "/_matrix/client/v3/rooms/{}/state/{}/{}",
            encode_segment(room_id),
            CALL_MEMBER_EVENT,
            encode_segment(state_key)
        )
    }

    /// Schedules the empty membership (the leave) to be sent after `delay_ms`; returns its
    /// `delay_id`.
    pub async fn arm_delayed_leave(&self, room_id: &str, state_key: &str, delay_ms: u64) -> Result<String, Error> {
        let path = format!(
            "{}?org.matrix.msc4140.delay={delay_ms}",
            Self::member_path(room_id, state_key)
        );
        let v = self.request(Method::PUT, &path, Some(&json!({}))).await?;
        v["delay_id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::Malformed(format!("no delay_id in {v}")))
    }

    /// Publishes membership state `content` (`{}` is a leave).
    pub async fn put_membership(&self, room_id: &str, state_key: &str, content: &Value) -> Result<(), Error> {
        self.request(Method::PUT, &Self::member_path(room_id, state_key), Some(content))
            .await?;
        Ok(())
    }

    /// Restarts, sends or cancels a delayed event.
    pub async fn delayed_action(&self, delay_id: &str, action: DelayedAction) -> Result<(), Error> {
        let path = format!(
            "/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{}",
            encode_segment(delay_id)
        );
        self.request(Method::POST, &path, Some(&json!({"action": action.as_str()})))
            .await?;
        Ok(())
    }

    /// Leaves: sends the delayed leave now if there is one, otherwise (or if that fails)
    /// publishes the empty membership directly.
    pub async fn clear_membership(&self, room_id: &str, state_key: &str, delay_id: Option<&str>) -> Result<(), Error> {
        if let Some(id) = delay_id {
            match self.delayed_action(id, DelayedAction::Send).await {
                Ok(()) => return Ok(()),
                Err(e) => tracing::warn!("sending the delayed leave failed ({e}); clearing membership directly"),
            }
        }
        self.put_membership(room_id, state_key, &json!({})).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::{
        matchers::{body_json, header, method, path, query_param},
        Mock, MockServer, ResponseTemplate,
    };

    const MEMBER_PATH: &str =
        "/_matrix/client/v3/rooms/%21r%3As/state/org.matrix.msc3401.call.member/_%40u%3As_DEV_m.call";

    async fn hs() -> (MockServer, Homeserver) {
        let server = MockServer::start().await;
        let hs = Homeserver::new(&server.uri(), "tok");
        (server, hs)
    }

    #[tokio::test]
    async fn discovers_first_livekit_transport() {
        let (server, hs) = hs().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .and(header("authorization", "Bearer tok"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"rtc_transports": [
                {"type": "other", "livekit_service_url": "https://no"},
                {"type": "livekit", "livekit_service_url": "https://lk.example/"},
                {"type": "livekit", "livekit_service_url": "https://second"}
            ]})))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(hs.livekit_service_url().await.unwrap(), "https://lk.example");
    }

    #[tokio::test]
    async fn no_livekit_transport_is_error() {
        let (server, hs) = hs().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"rtc_transports": []})))
            .mount(&server)
            .await;
        assert!(matches!(hs.livekit_service_url().await, Err(Error::NoLiveKitTransport)));
    }

    #[tokio::test]
    async fn openid_token_passed_through_verbatim() {
        let (server, hs) = hs().await;
        let token = json!({"access_token": "o", "token_type": "Bearer", "matrix_server_name": "s", "expires_in": 3600, "extra": [1]});
        Mock::given(method("POST"))
            .and(path("/_matrix/client/v3/user/%40u%3As/openid/request_token"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(token.clone()))
            .mount(&server)
            .await;
        assert_eq!(hs.openid_token("@u:s").await.unwrap(), token);
    }

    #[tokio::test]
    async fn arm_delayed_leave_puts_empty_state_with_delay_8000() {
        let (server, hs) = hs().await;
        Mock::given(method("PUT"))
            .and(path(MEMBER_PATH))
            .and(query_param("org.matrix.msc4140.delay", "8000"))
            .and(header("authorization", "Bearer tok"))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"delay_id": "d1"})))
            .expect(1)
            .mount(&server)
            .await;
        assert_eq!(
            hs.arm_delayed_leave("!r:s", "_@u:s_DEV_m.call", 8000).await.unwrap(),
            "d1"
        );
    }

    #[tokio::test]
    async fn arm_without_delay_id_is_malformed() {
        let (server, hs) = hs().await;
        Mock::given(method("PUT"))
            .and(path(MEMBER_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id": "$x"})))
            .mount(&server)
            .await;
        assert!(matches!(
            hs.arm_delayed_leave("!r:s", "_@u:s_DEV_m.call", 8000).await,
            Err(Error::Malformed(_))
        ));
    }

    #[tokio::test]
    async fn put_membership_sends_content() {
        let (server, hs) = hs().await;
        let content = json!({"application": "m.call"});
        Mock::given(method("PUT"))
            .and(path(MEMBER_PATH))
            .and(body_json(content.clone()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id": "$e"})))
            .expect(1)
            .mount(&server)
            .await;
        hs.put_membership("!r:s", "_@u:s_DEV_m.call", &content).await.unwrap();
    }

    #[tokio::test]
    async fn restart_posts_action_restart() {
        let (server, hs) = hs().await;
        Mock::given(method("POST"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4140/delayed_events/d1"))
            .and(body_json(json!({"action": "restart"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        hs.delayed_action("d1", DelayedAction::Restart).await.unwrap();
    }

    #[tokio::test]
    async fn http_error_carries_status_and_body() {
        let (server, hs) = hs().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(404).set_body_string("gone"))
            .mount(&server)
            .await;
        match hs.delayed_action("d1", DelayedAction::Restart).await {
            Err(Error::Http { status: 404, body, .. }) => assert_eq!(body, "gone"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn clear_membership_sends_delayed_event() {
        let (server, hs) = hs().await;
        Mock::given(method("POST"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4140/delayed_events/d1"))
            .and(body_json(json!({"action": "send"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        hs.clear_membership("!r:s", "_@u:s_DEV_m.call", Some("d1"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn clear_membership_falls_back_to_empty_state() {
        let (server, hs) = hs().await;
        Mock::given(method("POST"))
            .and(path("/_matrix/client/unstable/org.matrix.msc4140/delayed_events/d1"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"errcode": "M_NOT_FOUND"})))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path(MEMBER_PATH))
            .and(body_json(json!({})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"event_id": "$e"})))
            .expect(1)
            .mount(&server)
            .await;
        hs.clear_membership("!r:s", "_@u:s_DEV_m.call", Some("d1"))
            .await
            .unwrap();
    }

    #[test]
    fn encodes_path_segments() {
        assert_eq!(encode_segment("!r:s"), "%21r%3As");
        assert_eq!(encode_segment("a-b_c.d~e"), "a-b_c.d~e");
        assert_eq!(encode_segment("a/b c"), "a%2Fb%20c");
    }
}
