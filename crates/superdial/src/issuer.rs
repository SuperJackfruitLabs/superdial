//! LiveKit token issuers. The shipped one is SuperExchange's token service (C5); a development
//! adapter for the rtc-spike's lk-jwt-service sits behind the `dev-lk-jwt` feature.

#[cfg(feature = "dev-lk-jwt")]
pub mod dev;

use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(feature = "dev-lk-jwt")]
pub use dev::LkJwtDevIssuer;

/// C5 `POST {base}/v1/token` request.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TokenRequest {
    pub room_id: String,
    pub device_id: String,
    /// The homeserver's OpenID response, verbatim.
    pub openid_token: Value,
}

/// C5 `POST {base}/v1/delayed-leave` request.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DelayedLeaveRequest {
    pub room_id: String,
    pub device_id: String,
    pub delay_id: String,
    pub openid_token: Value,
}

/// What the issuer grants: where LiveKit is and how to join it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LiveKitGrant {
    pub url: String,
    pub jwt: String,
    /// `{user_id}:{device_id}`.
    pub identity: String,
    pub livekit_room: String,
}

/// Why the token service refused (C4 reasons, carried by C5's 403).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefusalReason {
    NotMember,
    NotAgentRoom,
    HarnessMode,
    CallsDisabled,
    Other(String),
}

impl RefusalReason {
    pub fn from_wire(s: &str) -> Self {
        match s {
            "not_member" => Self::NotMember,
            "not_agent_room" => Self::NotAgentRoom,
            "harness_mode" => Self::HarnessMode,
            "calls_disabled" => Self::CallsDisabled,
            other => Self::Other(other.to_owned()),
        }
    }
}

/// A token issuer failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IssuerError {
    #[error("refused: {0:?}")]
    Refused(RefusalReason),
    #[error("HTTP {status}: {body}")]
    Http { status: u16, body: String },
    #[error("{0}")]
    Transport(String),
    #[error("malformed response: {0}")]
    Malformed(String),
}

impl From<reqwest::Error> for IssuerError {
    fn from(e: reqwest::Error) -> Self {
        IssuerError::Transport(e.to_string())
    }
}

/// Swaps an OpenID token for a LiveKit grant, and takes over a delayed leave.
#[async_trait::async_trait]
pub trait TokenIssuer: Send + Sync {
    async fn token(&self, req: &TokenRequest) -> Result<LiveKitGrant, IssuerError>;
    async fn delayed_leave(&self, req: &DelayedLeaveRequest) -> Result<(), IssuerError>;
}

pub(crate) fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("reqwest client")
}

/// Sends `body` to `url`; returns the success body as text, or the error mapped per C5.
pub(crate) async fn post_json(http: &reqwest::Client, url: &str, body: &impl Serialize) -> Result<String, IssuerError> {
    let resp = http.post(url).json(body).send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if status.is_success() {
        return Ok(text);
    }
    if status.as_u16() == 403 {
        if let Ok(v) = serde_json::from_str::<Value>(&text) {
            if v["errcode"] == "M_FORBIDDEN" {
                if let Some(r) = v["reason"].as_str() {
                    return Err(IssuerError::Refused(RefusalReason::from_wire(r)));
                }
            }
        }
    }
    Err(IssuerError::Http {
        status: status.as_u16(),
        body: text,
    })
}

/// SuperExchange's token service (C5). Its base URL is the homeserver's `livekit_service_url`.
#[derive(Debug, Clone)]
pub struct SuperExchangeIssuer {
    base: String,
    http: reqwest::Client,
}

impl SuperExchangeIssuer {
    pub fn new(base_url: &str) -> Self {
        Self {
            base: base_url.trim_end_matches('/').to_owned(),
            http: http_client(),
        }
    }
}

#[async_trait::async_trait]
impl TokenIssuer for SuperExchangeIssuer {
    async fn token(&self, req: &TokenRequest) -> Result<LiveKitGrant, IssuerError> {
        let text = post_json(&self.http, &format!("{}/v1/token", self.base), req).await?;
        serde_json::from_str(&text).map_err(|e| IssuerError::Malformed(e.to_string()))
    }

    async fn delayed_leave(&self, req: &DelayedLeaveRequest) -> Result<(), IssuerError> {
        post_json(&self.http, &format!("{}/v1/delayed-leave", self.base), req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::{
        matchers::{body_json, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn openid() -> Value {
        json!({"access_token": "o", "token_type": "Bearer", "matrix_server_name": "s", "expires_in": 3600})
    }

    fn req() -> TokenRequest {
        TokenRequest {
            room_id: "!r:s".into(),
            device_id: "DEV".into(),
            openid_token: openid(),
        }
    }

    fn grant_json() -> Value {
        json!({"url": "wss://lk", "jwt": "j", "identity": "@u:s:DEV", "livekit_room": "abc"})
    }

    #[tokio::test]
    async fn token_posts_c5_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .and(body_json(
                json!({"room_id": "!r:s", "device_id": "DEV", "openid_token": openid()}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(grant_json()))
            .expect(1)
            .mount(&server)
            .await;
        SuperExchangeIssuer::new(&format!("{}/", server.uri()))
            .token(&req())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn token_parses_grant() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(grant_json()))
            .mount(&server)
            .await;
        let g = SuperExchangeIssuer::new(&server.uri()).token(&req()).await.unwrap();
        assert_eq!(
            g,
            LiveKitGrant {
                url: "wss://lk".into(),
                jwt: "j".into(),
                identity: "@u:s:DEV".into(),
                livekit_room: "abc".into()
            }
        );
    }

    async fn refusal(reason: &str) -> IssuerError {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/token"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({"errcode": "M_FORBIDDEN", "reason": reason})))
            .mount(&server)
            .await;
        SuperExchangeIssuer::new(&server.uri()).token(&req()).await.unwrap_err()
    }

    #[tokio::test]
    async fn refusal_maps_reason() {
        for (wire, reason) in [
            ("not_member", RefusalReason::NotMember),
            ("not_agent_room", RefusalReason::NotAgentRoom),
            ("harness_mode", RefusalReason::HarnessMode),
            ("calls_disabled", RefusalReason::CallsDisabled),
        ] {
            assert_eq!(refusal(wire).await, IssuerError::Refused(reason));
        }
    }

    #[tokio::test]
    async fn unknown_refusal_reason_kept() {
        assert_eq!(
            refusal("new_reason").await,
            IssuerError::Refused(RefusalReason::Other("new_reason".into()))
        );
    }

    #[tokio::test]
    async fn non_json_error_is_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(502).set_body_string("bad gateway"))
            .mount(&server)
            .await;
        let e = SuperExchangeIssuer::new(&server.uri()).token(&req()).await.unwrap_err();
        assert_eq!(
            e,
            IssuerError::Http {
                status: 502,
                body: "bad gateway".into()
            }
        );
        let e2 = SuperExchangeIssuer::new(&server.uri())
            .delayed_leave(&DelayedLeaveRequest {
                room_id: "!r:s".into(),
                device_id: "DEV".into(),
                delay_id: "d1".into(),
                openid_token: openid(),
            })
            .await
            .unwrap_err();
        assert_eq!(
            e2,
            IssuerError::Http {
                status: 502,
                body: "bad gateway".into()
            }
        );
    }

    #[tokio::test]
    async fn grant_missing_identity_is_malformed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"url": "wss://lk", "jwt": "j"})))
            .mount(&server)
            .await;
        let e = SuperExchangeIssuer::new(&server.uri()).token(&req()).await.unwrap_err();
        assert!(matches!(e, IssuerError::Malformed(_)));
    }

    #[tokio::test]
    async fn delayed_leave_posts_c5_body_and_accepts_204() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/delayed-leave"))
            .and(body_json(
                json!({"room_id": "!r:s", "device_id": "DEV", "delay_id": "d1", "openid_token": openid()}),
            ))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        SuperExchangeIssuer::new(&server.uri())
            .delayed_leave(&DelayedLeaveRequest {
                room_id: "!r:s".into(),
                device_id: "DEV".into(),
                delay_id: "d1".into(),
                openid_token: openid(),
            })
            .await
            .unwrap();
    }
}
