//! Logging in as a test user, with a persistent sqlite store and session per data directory.

use std::path::Path;

use anyhow::{Context, Result};
use matrix_sdk::{authentication::matrix::MatrixSession, Client};

/// Restores the session saved in `data_dir`, or logs in with the password in `password_file`
/// and saves it. The password is never logged.
pub async fn login(homeserver: &str, user: &str, password_file: &Path, data_dir: &Path) -> Result<Client> {
    std::fs::create_dir_all(data_dir)?;
    let client = Client::builder()
        .homeserver_url(homeserver)
        .sqlite_store(data_dir.join("store"), None)
        .build()
        .await?;
    let session_file = data_dir.join("session.json");
    if let Ok(s) = std::fs::read_to_string(&session_file) {
        let session: MatrixSession = serde_json::from_str(&s)?;
        client.restore_session(session).await?;
    } else {
        let password =
            std::fs::read_to_string(password_file).with_context(|| format!("read {}", password_file.display()))?;
        client
            .matrix_auth()
            .login_username(user, password.trim())
            .initial_device_display_name("superdial-testcaller")
            .await?;
        let session = client.matrix_auth().session().context("no session after login")?;
        std::fs::write(&session_file, serde_json::to_string(&session)?)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&session_file, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    Ok(client)
}
