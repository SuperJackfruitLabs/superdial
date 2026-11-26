//! superdial-agent: serves C6 to the AgentPod hub and joins calls as bridge agents' call devices.

use std::sync::Arc;

use anyhow::Context;
use superdial_agent::{
    agent::{Agent, AgentDeps},
    api,
    config::Config,
    hub::HubClient,
    speech::{HttpSynthesizer, HttpTranscriber},
};
use tracing_subscriber::EnvFilter;

/// Library noise kept down; none of these log secrets at warn. libwebrtc warns about every
/// ICE candidate it skips, and matrix-sdk-crypto about the missing key backup on every room
/// key the call device is sent (and then discards).
const DEFAULT_LOG: &str =
    "info,matrix_sdk=warn,matrix_sdk_base=warn,matrix_sdk_crypto=warn,matrix_sdk_crypto::backups=error,\
                           matrix_sdk_sqlite=warn,matrix_sdk_common=warn,livekit=warn,libwebrtc=error,livekit_api=warn,\
                           hyper=warn,reqwest=warn";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG)))
        .init();

    let cfg = Config::from_env().context("configuration")?;
    tracing::info!(config = ?cfg, version = env!("CARGO_PKG_VERSION"), "starting");
    if cfg.log_text {
        tracing::warn!("SUPERDIAL_AGENT_LOG_TEXT is on: transcripts and replies are logged (development only)");
    }
    std::fs::create_dir_all(&cfg.state_dir).with_context(|| format!("create {}", cfg.state_dir.display()))?;

    let agent = Agent::new(AgentDeps {
        state_dir: cfg.state_dir.clone(),
        store_passphrase: cfg.store_passphrase.clone(),
        stt: Arc::new(HttpTranscriber::new(&cfg.transcribe)),
        tts: Arc::new(HttpSynthesizer::new(&cfg.speech)),
        hub: Arc::new(HubClient::new(&cfg.hub_url, &cfg.token)),
        issuer: issuer(&cfg)?,
        log_text: cfg.log_text,
    });
    let app = api::router(agent.clone(), cfg.token.clone());
    let listener = tokio::net::TcpListener::bind(cfg.bind)
        .await
        .with_context(|| format!("bind {}", cfg.bind))?;
    tracing::info!(bind = %cfg.bind, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    tracing::info!(
        calls = api::CallLayer::calls(agent.as_ref()),
        "shutting down: leaving every call"
    );
    agent.shutdown().await;
    Ok(())
}

/// The token issuer: SuperExchange's, discovered per homeserver, unless a development build is
/// pointed at lk-jwt-service.
fn issuer(cfg: &Config) -> anyhow::Result<Option<Arc<dyn superdial::TokenIssuer>>> {
    match &cfg.dev_lk_jwt_url {
        None => Ok(None),
        #[cfg(feature = "dev-lk-jwt")]
        Some(url) => {
            tracing::warn!(url, "development token issuer (lk-jwt-service) in use");
            Ok(Some(Arc::new(superdial::LkJwtDevIssuer::new(url))))
        }
        #[cfg(not(feature = "dev-lk-jwt"))]
        Some(_) => anyhow::bail!("SUPERDIAL_AGENT_DEV_LK_JWT_URL needs a build with the dev-lk-jwt feature"),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
