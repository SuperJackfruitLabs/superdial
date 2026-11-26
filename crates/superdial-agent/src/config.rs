//! Configuration from the environment. Every secret may instead be read from a file named by
//! the same variable with `_FILE` appended.

use std::{fmt, io, net::SocketAddr, path::Path, path::PathBuf};

/// The default listen address: loopback only.
pub const DEFAULT_BIND: &str = "127.0.0.1:8850";
/// The default state directory (one crypto store per call device beneath it).
pub const DEFAULT_STATE_DIR: &str = "/var/lib/superdial-agent";
/// The model asked for when `TRANSCRIBE_MODEL` is unset: what foundry's transcriber serves.
pub const DEFAULT_TRANSCRIBE_MODEL: &str = "large-v3-turbo";

/// Where speech-to-text is and how to ask it.
#[derive(Clone)]
pub struct SttConfig {
    pub url: String,
    pub api_key: String,
    pub model: String,
}

/// Where text-to-speech is.
#[derive(Clone)]
pub struct TtsConfig {
    pub url: String,
    pub api_key: String,
}

/// Everything superdial-agent is configured with.
#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    /// `SUPERDIAL_AGENT_TOKEN`: the bearer both directions of C6 use.
    pub token: String,
    pub hub_url: String,
    pub state_dir: PathBuf,
    pub transcribe: SttConfig,
    pub speech: TtsConfig,
    /// Encrypts the per-device sqlite stores at rest when set.
    pub store_passphrase: Option<String>,
    /// Development only: allow transcript and reply text in logs.
    pub log_text: bool,
    /// Development only (feature `dev-lk-jwt`): lk-jwt-service base URL used as token issuer.
    pub dev_lk_jwt_url: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{0} is not set")]
    Missing(String),
    #[error("set {0} or {0}_FILE, not both")]
    Both(String),
    #[error("{name}_FILE: {err}")]
    File { name: String, err: String },
    #[error("{name}: {why}")]
    Invalid { name: String, why: String },
}

type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;
type ReadFile<'a> = &'a dyn Fn(&Path) -> io::Result<String>;

impl Config {
    /// Reads the process environment.
    pub fn from_env() -> Result<Config, ConfigError> {
        Self::from_lookup(&|k| std::env::var(k).ok(), &|p| std::fs::read_to_string(p))
    }

    /// Reads configuration through `get` (an environment) and `read` (for `*_FILE`).
    pub fn from_lookup(get: Lookup, read: ReadFile) -> Result<Config, ConfigError> {
        let plain = |name: &str| get(name).map(|v| v.trim().to_owned()).filter(|v| !v.is_empty());
        let required = |name: &str| plain(name).ok_or_else(|| ConfigError::Missing(name.into()));
        let secret = |name: &str| -> Result<Option<String>, ConfigError> {
            let file_var = format!("{name}_FILE");
            match (plain(name), plain(&file_var)) {
                (Some(_), Some(_)) => Err(ConfigError::Both(name.into())),
                (Some(v), None) => Ok(Some(v)),
                (None, Some(path)) => {
                    let v = read(Path::new(&path)).map_err(|e| ConfigError::File {
                        name: name.into(),
                        err: e.kind().to_string(),
                    })?;
                    Ok(Some(v.trim().to_owned()).filter(|v| !v.is_empty()))
                }
                (None, None) => Ok(None),
            }
        };
        let required_secret = |name: &str| secret(name)?.ok_or_else(|| ConfigError::Missing(name.into()));

        let bind_text = plain("SUPERDIAL_AGENT_BIND").unwrap_or_else(|| DEFAULT_BIND.into());
        let bind = bind_text.parse().map_err(|_| ConfigError::Invalid {
            name: "SUPERDIAL_AGENT_BIND".into(),
            why: format!("{bind_text:?} is not an address and port"),
        })?;
        Ok(Config {
            bind,
            token: required_secret("SUPERDIAL_AGENT_TOKEN")?,
            hub_url: required("SUPERDIAL_HUB_URL")?,
            state_dir: plain("SUPERDIAL_STATE_DIR")
                .unwrap_or_else(|| DEFAULT_STATE_DIR.into())
                .into(),
            transcribe: SttConfig {
                url: required("TRANSCRIBE_URL")?,
                api_key: required_secret("TRANSCRIBE_API_KEY")?,
                model: plain("TRANSCRIBE_MODEL").unwrap_or_else(|| DEFAULT_TRANSCRIBE_MODEL.into()),
            },
            speech: TtsConfig {
                url: required("SPEECH_URL")?,
                api_key: required_secret("SPEECH_API_KEY")?,
            },
            store_passphrase: secret("SUPERDIAL_AGENT_STORE_PASSPHRASE")?,
            log_text: matches!(plain("SUPERDIAL_AGENT_LOG_TEXT").as_deref(), Some("1" | "true" | "yes")),
            dev_lk_jwt_url: plain("SUPERDIAL_AGENT_DEV_LK_JWT_URL"),
        })
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("bind", &self.bind)
            .field("token", &"<redacted>")
            .field("hub_url", &self.hub_url)
            .field("state_dir", &self.state_dir)
            .field("transcribe_url", &self.transcribe.url)
            .field("transcribe_model", &self.transcribe.model)
            .field("speech_url", &self.speech.url)
            .field(
                "store_passphrase",
                &self.store_passphrase.as_ref().map(|_| "<redacted>"),
            )
            .field("log_text", &self.log_text)
            .field("dev_lk_jwt_url", &self.dev_lk_jwt_url)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn base() -> HashMap<String, String> {
        [
            ("SUPERDIAL_AGENT_TOKEN", "loopback-secret-1"),
            ("SUPERDIAL_HUB_URL", "http://127.0.0.1:3000"),
            ("SUPERDIAL_STATE_DIR", "/state"),
            ("TRANSCRIBE_URL", "http://foundry:8840"),
            ("TRANSCRIBE_API_KEY", "stt-key-2"),
            ("SPEECH_URL", "http://foundry:8841"),
            ("SPEECH_API_KEY", "tts-key-3"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
    }

    fn load(env: &HashMap<String, String>, files: &HashMap<&str, &str>) -> Result<Config, ConfigError> {
        Config::from_lookup(&|k| env.get(k).cloned(), &|p| {
            files
                .get(p.to_str().unwrap())
                .map(|s| s.to_string())
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such file"))
        })
    }

    #[test]
    fn secret_from_env() {
        let c = load(&base(), &HashMap::new()).unwrap();
        assert_eq!(c.token, "loopback-secret-1");
        assert_eq!(c.transcribe.api_key, "stt-key-2");
        assert_eq!(c.speech.api_key, "tts-key-3");
        assert_eq!(c.hub_url, "http://127.0.0.1:3000");
        assert_eq!(c.state_dir, PathBuf::from("/state"));
        assert_eq!(c.transcribe.url, "http://foundry:8840");
        assert_eq!(c.speech.url, "http://foundry:8841");
        assert!(c.store_passphrase.is_none());
        assert!(!c.log_text);
    }

    #[test]
    fn secret_from_file_trimmed() {
        let mut env = base();
        env.remove("SUPERDIAL_AGENT_TOKEN");
        env.remove("SPEECH_API_KEY");
        env.insert("SUPERDIAL_AGENT_TOKEN_FILE".into(), "/run/secrets/token".into());
        env.insert("SPEECH_API_KEY_FILE".into(), "/run/secrets/speech".into());
        env.insert(
            "SUPERDIAL_AGENT_STORE_PASSPHRASE_FILE".into(),
            "/run/secrets/pass".into(),
        );
        let files = HashMap::from([
            ("/run/secrets/token", "from-file\n"),
            ("/run/secrets/speech", "  speech-file  \n"),
            ("/run/secrets/pass", "pass\n"),
        ]);
        let c = load(&env, &files).unwrap();
        assert_eq!(c.token, "from-file");
        assert_eq!(c.speech.api_key, "speech-file");
        assert_eq!(c.store_passphrase.as_deref(), Some("pass"));
    }

    #[test]
    fn secret_both_set_is_error() {
        let mut env = base();
        env.insert("TRANSCRIBE_API_KEY_FILE".into(), "/x".into());
        assert_eq!(
            load(&env, &HashMap::new()).unwrap_err(),
            ConfigError::Both("TRANSCRIBE_API_KEY".into())
        );
    }

    #[test]
    fn unreadable_secret_file_is_error() {
        let mut env = base();
        env.remove("SPEECH_API_KEY");
        env.insert("SPEECH_API_KEY_FILE".into(), "/missing".into());
        assert!(matches!(load(&env, &HashMap::new()), Err(ConfigError::File { name, .. }) if name == "SPEECH_API_KEY"));
    }

    #[test]
    fn missing_required_is_error_naming_variable() {
        for name in [
            "SUPERDIAL_AGENT_TOKEN",
            "SUPERDIAL_HUB_URL",
            "TRANSCRIBE_URL",
            "TRANSCRIBE_API_KEY",
            "SPEECH_URL",
            "SPEECH_API_KEY",
        ] {
            let mut env = base();
            env.remove(name);
            assert_eq!(
                load(&env, &HashMap::new()).unwrap_err(),
                ConfigError::Missing(name.into()),
                "{name}"
            );
        }
        let mut env = base();
        env.insert("SPEECH_URL".into(), "  ".into());
        assert_eq!(
            load(&env, &HashMap::new()).unwrap_err(),
            ConfigError::Missing("SPEECH_URL".into())
        );
    }

    #[test]
    fn defaults_bind_and_model() {
        let mut env = base();
        env.remove("SUPERDIAL_STATE_DIR");
        let c = load(&env, &HashMap::new()).unwrap();
        assert_eq!(c.bind, DEFAULT_BIND.parse::<SocketAddr>().unwrap());
        assert_eq!(c.transcribe.model, "large-v3-turbo");
        assert_eq!(c.state_dir, PathBuf::from(DEFAULT_STATE_DIR));
        env.insert("SUPERDIAL_AGENT_BIND".into(), "0.0.0.0:9000".into());
        env.insert("TRANSCRIBE_MODEL".into(), "small".into());
        env.insert("SUPERDIAL_AGENT_LOG_TEXT".into(), "1".into());
        let c = load(&env, &HashMap::new()).unwrap();
        assert_eq!(c.bind, "0.0.0.0:9000".parse::<SocketAddr>().unwrap());
        assert_eq!(c.transcribe.model, "small");
        assert!(c.log_text);
    }

    #[test]
    fn bad_bind_is_invalid() {
        let mut env = base();
        env.insert("SUPERDIAL_AGENT_BIND".into(), "nope".into());
        assert!(
            matches!(load(&env, &HashMap::new()), Err(ConfigError::Invalid { name, .. }) if name == "SUPERDIAL_AGENT_BIND")
        );
    }

    #[test]
    fn debug_redacts_secrets() {
        let mut env = base();
        env.insert("SUPERDIAL_AGENT_STORE_PASSPHRASE".into(), "pass-phrase-4".into());
        let c = load(&env, &HashMap::new()).unwrap();
        let s = format!("{c:?}");
        for secret in ["loopback-secret-1", "stt-key-2", "tts-key-3", "pass-phrase-4"] {
            assert!(!s.contains(secret), "{secret} leaked: {s}");
        }
        assert!(s.contains("http://foundry:8840"));
    }
}
