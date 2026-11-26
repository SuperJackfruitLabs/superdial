//! The hub's side of C6 that superdial-agent calls: cancelling a spoken turn.

use std::time::Duration;

use async_trait::async_trait;
use matrix_sdk::ruma::RoomId;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum HubError {
    #[error("the hub answered {status}: {detail}")]
    Http { status: u16, detail: String },
    #[error("the hub could not be reached: {0}")]
    Transport(String),
}

/// Asks the hub to stop a spoken turn (it maps this to `cancelTurn`).
#[async_trait]
pub trait TurnCanceller: Send + Sync {
    async fn cancel(&self, room_id: &RoomId, turn_id: &str) -> Result<(), HubError>;
}

/// `POST {SUPERDIAL_HUB_URL}/internal/superdial/turns/{turn_id}/cancel` with `{"room_id"}` and
/// bearer `SUPERDIAL_AGENT_TOKEN`.
pub struct HubClient {
    base: String,
    token: String,
    http: reqwest::Client,
}

impl HubClient {
    pub fn new(hub_url: &str, token: &str) -> Self {
        Self {
            base: hub_url.trim_end_matches('/').to_owned(),
            token: token.to_owned(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }
}

#[async_trait]
impl TurnCanceller for HubClient {
    async fn cancel(&self, room_id: &RoomId, turn_id: &str) -> Result<(), HubError> {
        let url = format!("{}/internal/superdial/turns/{turn_id}/cancel", self.base);
        let res = self
            .http
            .post(url)
            .bearer_auth(&self.token)
            .json(&json!({"room_id": room_id}))
            .send()
            .await
            .map_err(|e| HubError::Transport(e.without_url().to_string()))?;
        let status = res.status();
        if status.is_success() {
            return Ok(());
        }
        let body = res.text().await.unwrap_or_default();
        Err(HubError::Http {
            status: status.as_u16(),
            detail: crate::speech::detail(&body),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        matchers::{body_json, header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    const TURN: &str = "0b6c7c4e-3f0e-4c1b-9a43-6f1d2a9e5b77";

    fn room() -> &'static RoomId {
        <&RoomId>::try_from("!r:s").unwrap()
    }

    #[tokio::test]
    async fn cancel_posts_room_id_with_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path(format!("/internal/superdial/turns/{TURN}/cancel")))
            .and(header("authorization", "Bearer loop"))
            .and(body_json(json!({"room_id": "!r:s"})))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;
        HubClient::new(&format!("{}/", server.uri()), "loop")
            .cancel(room(), TURN)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cancel_non_2xx_is_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("bad token"))
            .mount(&server)
            .await;
        assert!(matches!(
            HubClient::new(&server.uri(), "loop").cancel(room(), TURN).await,
            Err(HubError::Http { status: 401, .. })
        ));
    }

    #[tokio::test]
    async fn cancel_unreachable_is_transport_error() {
        assert!(matches!(
            HubClient::new("http://127.0.0.1:9", "loop").cancel(room(), TURN).await,
            Err(HubError::Transport(_))
        ));
    }
}
