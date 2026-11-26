//! The suite's speech services on foundry, through their OpenAI-compatible APIs:
//! `POST /v1/audio/transcriptions` (faster-whisper) and `POST /v1/audio/speech` (Kokoro). The
//! request shapes are the hub's (`agentpod/apps/hub/src/services/speech`). superdial-agent uses
//! its own key for both. Neither the audio nor the text is ever logged here.

use std::time::Duration;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::{
    audio::{self, STT_RATE},
    config::{SttConfig, TtsConfig},
};

/// Five minutes of audio takes about 82 s on foundry, and a queue can double it (the hub's limit).
const STT_TIMEOUT: Duration = Duration::from_secs(6 * 60);
/// The speech service's own deadline is 120 s (the hub's limit).
const TTS_TIMEOUT: Duration = Duration::from_secs(150);

/// What speech-to-text heard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heard {
    pub text: String,
    /// The language it heard, when it said.
    pub language: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum SpeechError {
    #[error("the speech service answered {status}: {detail}")]
    Http { status: u16, detail: String },
    #[error("the speech service could not be reached: {0}")]
    Transport(String),
    #[error("the speech service answered something unreadable: {0}")]
    Malformed(String),
}

/// Speech to text.
#[async_trait]
pub trait Transcriber: Send + Sync {
    /// Transcribes 16 kHz mono PCM, in `language` when given.
    async fn transcribe(&self, pcm16k: &[i16], language: Option<&str>) -> Result<Heard, SpeechError>;
}

/// Text to speech.
#[async_trait]
pub trait Synthesizer: Send + Sync {
    /// Speaks `text` in `voice`, as 48 kHz mono PCM.
    async fn synthesize(&self, text: &str, voice: &str) -> Result<Vec<i16>, SpeechError>;
}

fn http_client(timeout: Duration) -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(timeout)
        .build()
        .expect("reqwest client")
}

/// At most 200 characters of an error body, as the hub keeps.
pub(crate) fn detail(body: &str) -> String {
    body.trim().chars().take(200).collect()
}

async fn check(res: reqwest::Response) -> Result<reqwest::Response, SpeechError> {
    let status = res.status();
    if status.is_success() {
        return Ok(res);
    }
    let body = res.text().await.unwrap_or_default();
    Err(SpeechError::Http {
        status: status.as_u16(),
        detail: detail(&body),
    })
}

fn transport(e: reqwest::Error) -> SpeechError {
    SpeechError::Transport(e.without_url().to_string())
}

fn base(url: &str) -> String {
    url.trim_end_matches('/').to_owned()
}

/// foundry's agentpod-transcriber, or any OpenAI-compatible transcription service.
pub struct HttpTranscriber {
    url: String,
    api_key: String,
    model: String,
    http: reqwest::Client,
}

impl HttpTranscriber {
    pub fn new(cfg: &SttConfig) -> Self {
        Self {
            url: format!("{}/v1/audio/transcriptions", base(&cfg.url)),
            api_key: cfg.api_key.clone(),
            model: cfg.model.clone(),
            http: http_client(STT_TIMEOUT),
        }
    }
}

#[async_trait]
impl Transcriber for HttpTranscriber {
    async fn transcribe(&self, pcm16k: &[i16], language: Option<&str>) -> Result<Heard, SpeechError> {
        let file = reqwest::multipart::Part::bytes(audio::wav_bytes(pcm16k, STT_RATE))
            .file_name("turn.wav")
            .mime_str("audio/wav")
            .map_err(transport)?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", file)
            .text("model", self.model.clone())
            .text("response_format", "json");
        if let Some(lang) = language.filter(|l| !l.is_empty()) {
            form = form.text("language", lang.to_owned());
        }
        let res = self
            .http
            .post(&self.url)
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(transport)?;
        let body: Value = check(res)
            .await?
            .json()
            .await
            .map_err(|e| SpeechError::Malformed(e.without_url().to_string()))?;
        let text = body["text"]
            .as_str()
            .ok_or_else(|| SpeechError::Malformed("no text".into()))?;
        Ok(Heard {
            text: text.trim().to_owned(),
            language: body["language"].as_str().map(str::to_owned),
        })
    }
}

/// foundry's agentpod-speech, or any OpenAI-compatible speech service.
pub struct HttpSynthesizer {
    url: String,
    api_key: String,
    http: reqwest::Client,
}

impl HttpSynthesizer {
    pub fn new(cfg: &TtsConfig) -> Self {
        Self {
            url: format!("{}/v1/audio/speech", base(&cfg.url)),
            api_key: cfg.api_key.clone(),
            http: http_client(TTS_TIMEOUT),
        }
    }
}

#[async_trait]
impl Synthesizer for HttpSynthesizer {
    async fn synthesize(&self, text: &str, voice: &str) -> Result<Vec<i16>, SpeechError> {
        let res = self
            .http
            .post(&self.url)
            .bearer_auth(&self.api_key)
            // `model` is ignored by agentpod-speech; OpenAI-compatible services need one.
            .json(&json!({"model": "tts-1", "input": text, "voice": voice, "response_format": "wav"}))
            .send()
            .await
            .map_err(transport)?;
        let bytes = check(res).await?.bytes().await.map_err(transport)?;
        audio::wav_to_mono_48k(&bytes).map_err(|e| SpeechError::Malformed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        matchers::{body_json, body_string_contains, header, method, path},
        Mock, MockServer, ResponseTemplate,
    };

    fn stt(server: &MockServer) -> HttpTranscriber {
        HttpTranscriber::new(&SttConfig {
            url: format!("{}/", server.uri()),
            api_key: "stt-key".into(),
            model: "large-v3-turbo".into(),
        })
    }

    fn tts(server: &MockServer) -> HttpSynthesizer {
        HttpSynthesizer::new(&TtsConfig {
            url: server.uri(),
            api_key: "tts-key".into(),
        })
    }

    async fn transcription_server(reply: Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/audio/transcriptions"))
            .and(header("authorization", "Bearer stt-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(reply))
            .mount(&server)
            .await;
        server
    }

    async fn body(server: &MockServer) -> String {
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        String::from_utf8_lossy(&reqs[0].body).into_owned()
    }

    #[tokio::test]
    async fn transcribe_posts_multipart_with_model_and_bearer() {
        let server = transcription_server(json!({"text": " Hello agent. ", "language": "en"})).await;
        stt(&server).transcribe(&[0; 1600], None).await.unwrap();
        let reqs = server.received_requests().await.unwrap();
        let ct = reqs[0]
            .headers
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        assert!(ct.starts_with("multipart/form-data"), "{ct}");
        let b = body(&server).await;
        assert!(b.contains("name=\"model\"\r\n\r\nlarge-v3-turbo"), "{b}");
        assert!(b.contains("name=\"response_format\"\r\n\r\njson"));
        assert!(b.contains("name=\"file\"; filename=\"turn.wav\""));
        assert!(b.contains("audio/wav"));
        assert!(b.contains("RIFF"));
    }

    #[tokio::test]
    async fn transcribe_sends_language_when_given() {
        let server = transcription_server(json!({"text": "namaste"})).await;
        stt(&server).transcribe(&[0; 160], Some("hi")).await.unwrap();
        assert!(body(&server).await.contains("name=\"language\"\r\n\r\nhi"));
    }

    #[tokio::test]
    async fn transcribe_omits_language_when_none() {
        let server = transcription_server(json!({"text": "x"})).await;
        stt(&server).transcribe(&[0; 160], None).await.unwrap();
        assert!(!body(&server).await.contains("name=\"language\""));
    }

    #[tokio::test]
    async fn transcribe_reads_text_and_language() {
        let server = transcription_server(json!({"text": " Hello agent. ", "language": "en", "duration": 2.1})).await;
        let heard = stt(&server).transcribe(&[0; 160], None).await.unwrap();
        assert_eq!(
            heard,
            Heard {
                text: "Hello agent.".into(),
                language: Some("en".into())
            }
        );
        let server = transcription_server(json!({"text": "hi"})).await;
        assert_eq!(stt(&server).transcribe(&[0; 160], None).await.unwrap().language, None);
    }

    #[tokio::test]
    async fn transcribe_without_text_is_malformed() {
        let server = transcription_server(json!({"language": "en"})).await;
        assert!(matches!(
            stt(&server).transcribe(&[0; 160], None).await,
            Err(SpeechError::Malformed(_))
        ));
    }

    #[tokio::test]
    async fn transcribe_http_error_keeps_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("{\"detail\":\"unauthorized\"}"))
            .mount(&server)
            .await;
        match stt(&server).transcribe(&[0; 160], None).await {
            Err(SpeechError::Http { status, detail }) => {
                assert_eq!(status, 401);
                assert!(detail.contains("unauthorized"));
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn synthesize_posts_json_and_returns_48k() {
        let server = MockServer::start().await;
        let pcm24: Vec<i16> = (0..2400).map(|i| (i % 50) as i16).collect();
        Mock::given(method("POST"))
            .and(path("/v1/audio/speech"))
            .and(header("authorization", "Bearer tts-key"))
            .and(body_json(
                json!({"model": "tts-1", "input": "Hi.", "voice": "af_heart", "response_format": "wav"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(audio::wav_bytes(&pcm24, 24_000)))
            .mount(&server)
            .await;
        let out = tts(&server).synthesize("Hi.", "af_heart").await.unwrap();
        assert_eq!(out.len(), 4800);
    }

    #[tokio::test]
    async fn synthesize_error_status() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_string_contains("af_heart"))
            .respond_with(ResponseTemplate::new(503).set_body_string("busy; try again shortly"))
            .mount(&server)
            .await;
        assert!(matches!(
            tts(&server).synthesize("Hi.", "af_heart").await,
            Err(SpeechError::Http { status: 503, .. })
        ));
    }

    #[tokio::test]
    async fn synthesize_non_wav_is_malformed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"OggS....".to_vec()))
            .mount(&server)
            .await;
        assert!(matches!(
            tts(&server).synthesize("Hi.", "af_heart").await,
            Err(SpeechError::Malformed(_))
        ));
    }

    #[test]
    fn long_error_detail_is_truncated() {
        assert_eq!(detail(&"x".repeat(500)).chars().count(), 200);
    }
}
