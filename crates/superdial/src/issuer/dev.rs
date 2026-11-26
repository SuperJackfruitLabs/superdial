//! Development-only issuer: the rtc-spike's lk-jwt-service (`POST /sfu/get`). It is reached
//! over HTTP only and must never be a shipped default; SuperExchange's token service replaces it.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::{json, Value};

use super::{http_client, post_json, DelayedLeaveRequest, IssuerError, LiveKitGrant, TokenIssuer, TokenRequest};

/// lk-jwt-service as a token issuer, for development against the rtc-spike stack.
#[derive(Debug, Clone)]
pub struct LkJwtDevIssuer {
    base: String,
    http: reqwest::Client,
}

impl LkJwtDevIssuer {
    pub fn new(base_url: &str) -> Self {
        Self {
            base: base_url.trim_end_matches('/').to_owned(),
            http: http_client(),
        }
    }
}

/// The claims of a JWT, decoded without verifying it (the issuer already did, and LiveKit will).
pub(crate) fn jwt_claims(jwt: &str) -> Result<Value, IssuerError> {
    let payload = jwt
        .split('.')
        .nth(1)
        .ok_or_else(|| IssuerError::Malformed("JWT has no payload".into()))?;
    let bytes = URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .map_err(|e| IssuerError::Malformed(format!("JWT payload: {e}")))?;
    serde_json::from_slice(&bytes).map_err(|e| IssuerError::Malformed(format!("JWT claims: {e}")))
}

#[async_trait::async_trait]
impl TokenIssuer for LkJwtDevIssuer {
    async fn token(&self, req: &TokenRequest) -> Result<LiveKitGrant, IssuerError> {
        let body = json!({"room": req.room_id, "openid_token": req.openid_token, "device_id": req.device_id});
        let text = post_json(&self.http, &format!("{}/sfu/get", self.base), &body).await?;
        let v: Value = serde_json::from_str(&text).map_err(|e| IssuerError::Malformed(e.to_string()))?;
        let (Some(url), Some(jwt)) = (v["url"].as_str(), v["jwt"].as_str()) else {
            return Err(IssuerError::Malformed(format!("no url/jwt in {v}")));
        };
        let claims = jwt_claims(jwt)?;
        let field = |name: &str, x: &Value| {
            x.as_str()
                .map(str::to_owned)
                .ok_or_else(|| IssuerError::Malformed(format!("JWT has no {name}")))
        };
        Ok(LiveKitGrant {
            url: url.to_owned(),
            jwt: jwt.to_owned(),
            identity: field("sub", &claims["sub"])?,
            livekit_room: field("video.room", &claims["video"]["room"])?,
        })
    }

    /// lk-jwt-service takes no hand-off here: the client's own restart loop is the only keeper.
    async fn delayed_leave(&self, req: &DelayedLeaveRequest) -> Result<(), IssuerError> {
        tracing::debug!(delay_id = %req.delay_id, "dev issuer: no delayed-leave hand-off");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    use serde_json::json;
    use wiremock::{
        matchers::{body_json, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn jwt(claims: Value) -> String {
        let h = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let p = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        format!("{h}.{p}.c2ln")
    }

    #[tokio::test]
    async fn dev_issuer_posts_sfu_get_and_reads_claims() {
        let server = MockServer::start().await;
        let openid =
            json!({"access_token": "o", "token_type": "Bearer", "matrix_server_name": "s", "expires_in": 3600});
        let token = jwt(json!({"sub": "@u:s:DEV", "video": {"room": "lkroom", "roomJoin": true}}));
        Mock::given(method("POST"))
            .and(path("/sfu/get"))
            .and(body_json(
                json!({"room": "!r:s", "openid_token": openid, "device_id": "DEV"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"url": "wss://sfu", "jwt": token})))
            .expect(1)
            .mount(&server)
            .await;
        let g = LkJwtDevIssuer::new(&server.uri())
            .token(&TokenRequest {
                room_id: "!r:s".into(),
                device_id: "DEV".into(),
                openid_token: openid,
            })
            .await
            .unwrap();
        assert_eq!(g.url, "wss://sfu");
        assert_eq!(g.jwt, token);
        assert_eq!(g.identity, "@u:s:DEV");
        assert_eq!(g.livekit_room, "lkroom");
    }

    #[test]
    fn claims_of_garbage_jwt_is_malformed() {
        assert!(matches!(jwt_claims("nope"), Err(IssuerError::Malformed(_))));
    }
}
