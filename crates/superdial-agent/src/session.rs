//! One matrix-sdk client per call device, shared by that device's calls: restored from the
//! hub's credentials, a persistent sqlite crypto store per device, and a sync loop that asks for
//! no room timelines, only the call rooms' membership state, plus to-device events. It routes
//! to-device events to the device's calls and discards the room keys the device is sent.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use matrix_sdk::{
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    ruma::{
        api::client::{
            filter::{FilterDefinition, LazyLoadOptions, RoomEventFilter},
            sync::sync_events::v3::Filter,
        },
        DeviceId, OwnedRoomId, RoomId, UInt, UserId,
    },
    Client, RoomState, SessionMeta, SessionTokens,
};
use superdial::{Call, CALL_MEMBER_EVENT};
use tokio::{
    sync::{watch, RwLock},
    task::JoinHandle,
};

/// The state the library needs from each call room: call memberships, and room memberships to
/// check that a call member is joined.
pub const SYNCED_STATE: [&str; 2] = [CALL_MEMBER_EVENT, "m.room.member"];
/// The sync long-poll. A new call room for a device already syncing waits at most this long.
const SYNC_TIMEOUT: Duration = Duration::from_secs(5);
/// matrix-sdk-sqlite's crypto database in a store directory.
const CRYPTO_DB: &str = "matrix-sdk-crypto.sqlite3";

/// The sync filter for a device whose calls are in `rooms`.
pub fn sync_filter(rooms: &[OwnedRoomId]) -> FilterDefinition {
    let types = || Some(SYNCED_STATE.iter().map(|t| t.to_string()).collect());
    let mut timeline = RoomEventFilter::default();
    // No timeline: with a limit of 0 the server reports state changes in `state`; should it
    // put any in the timeline, they are only these state types, never messages.
    timeline.limit = Some(UInt::MIN);
    timeline.types = types();
    let mut state = RoomEventFilter::default();
    state.types = types();
    state.lazy_load_options = LazyLoadOptions::Disabled;

    let mut def = FilterDefinition::ignore_all();
    def.room.rooms = Some(rooms.to_vec());
    def.room.timeline = timeline;
    def.room.state = state;
    def.room.ephemeral = RoomEventFilter::ignore_all();
    def.room.account_data = RoomEventFilter::ignore_all();
    def.room.include_leave = false;
    def
}

/// Where a call device's store lives: one directory per (user, device), named so that no
/// user or device id can escape `state_dir`.
pub fn store_dir(state_dir: &Path, user: &UserId, device: &DeviceId) -> PathBuf {
    fn safe(s: &str) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' => (b as char).to_string(),
                _ => format!("%{b:02X}"),
            })
            .collect()
    }
    state_dir
        .join("devices")
        .join(format!("{}~{}", safe(user.as_str()), safe(device.as_str())))
}

/// Deletes every Megolm room key in the device's crypto store. superdial-agent never reads
/// room messages, so it keeps none of the keys it is sent as a device of the agent user.
/// Returns how many were deleted; a store that does not exist yet has none.
pub fn purge_room_keys(store_dir: &Path) -> rusqlite::Result<usize> {
    let path = store_dir.join(CRYPTO_DB);
    if !path.exists() {
        return Ok(0);
    }
    let db = rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.busy_timeout(Duration::from_secs(5))?;
    db.execute("DELETE FROM inbound_group_session", [])
}

/// A call device's client and sync loop, and the calls it carries.
pub struct DeviceSession {
    client: Client,
    calls: Arc<RwLock<HashMap<OwnedRoomId, Call>>>,
    rooms: watch::Sender<Vec<OwnedRoomId>>,
    stop: watch::Sender<bool>,
    task: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl DeviceSession {
    /// Restores the device's session from the hub's credentials (no login, no network) and
    /// starts its sync loop.
    pub async fn start(
        homeserver_url: &str,
        user: &UserId,
        device: &DeviceId,
        access_token: &str,
        store_dir: PathBuf,
        passphrase: Option<&str>,
    ) -> anyhow::Result<Arc<DeviceSession>> {
        std::fs::create_dir_all(&store_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&store_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let client = Client::builder()
            .homeserver_url(homeserver_url)
            .sqlite_store(&store_dir, passphrase)
            .build()
            .await?;
        client
            .restore_session(MatrixSession {
                meta: SessionMeta {
                    user_id: user.to_owned(),
                    device_id: device.to_owned(),
                },
                tokens: SessionTokens {
                    access_token: access_token.to_owned(),
                    refresh_token: None,
                },
            })
            .await?;
        let calls: Arc<RwLock<HashMap<OwnedRoomId, Call>>> = Arc::default();
        let (rooms, rooms_rx) = watch::channel(Vec::new());
        let (stop, stop_rx) = watch::channel(false);
        let task = tokio::spawn(sync_loop(client.clone(), calls.clone(), rooms_rx, stop_rx, store_dir));
        Ok(Arc::new(DeviceSession {
            client,
            calls,
            rooms,
            stop,
            task: std::sync::Mutex::new(Some(task)),
        }))
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Routes this device's to-device events to `call` from now on, and syncs its room.
    pub async fn add_call(&self, room: OwnedRoomId, call: Call) {
        let mut calls = self.calls.write().await;
        calls.insert(room, call);
        self.rooms.send_replace(calls.keys().cloned().collect());
    }

    /// Stops routing to `room`'s call. Returns how many calls the device still carries.
    pub async fn remove_call(&self, room: &RoomId) -> usize {
        let mut calls = self.calls.write().await;
        calls.remove(room);
        self.rooms.send_replace(calls.keys().cloned().collect());
        calls.len()
    }

    /// Waits until sync has the room and the agent is joined to it.
    pub async fn wait_for_room(&self, room: &RoomId, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self
                .client
                .get_room(room)
                .is_some_and(|r| r.state() == RoomState::Joined)
            {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Stops the sync loop and waits for it (at most one long-poll).
    pub async fn stop(&self) {
        self.stop.send_replace(true);
        let task = self.task.lock().expect("task lock").take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

async fn sync_loop(
    client: Client,
    calls: Arc<RwLock<HashMap<OwnedRoomId, Call>>>,
    mut rooms: watch::Receiver<Vec<OwnedRoomId>>,
    stop: watch::Receiver<bool>,
    store_dir: PathBuf,
) {
    let who = client.user_id().map(|u| u.to_string()).unwrap_or_default();
    let mut filter = None;
    let mut fresh = true;
    let mut failures = 0u32;
    while !*stop.borrow() {
        if rooms.has_changed().unwrap_or(false) || filter.is_none() {
            filter = Some(sync_filter(&rooms.borrow_and_update()));
            // A room new to this loop needs its whole (filtered) state, not just changes.
            fresh = true;
        }
        let mut settings = SyncSettings::default().filter(Filter::FilterDefinition(filter.clone().expect("set")));
        if fresh {
            settings = settings.full_state(true).timeout(Duration::ZERO);
        } else {
            settings = settings.timeout(SYNC_TIMEOUT);
        }
        match client.sync_once(settings).await {
            Ok(resp) => {
                fresh = false;
                failures = 0;
                let timeline_events: usize = resp.rooms.joined.values().map(|r| r.timeline.events.len()).sum();
                // Event types only (never content), to show the filter holds.
                let timeline_types: Vec<String> = resp
                    .rooms
                    .joined
                    .values()
                    .flat_map(|r| r.timeline.events.iter())
                    .filter_map(|e| e.raw().get_field::<String>("type").ok().flatten())
                    .collect();
                let to_device = resp.to_device.len();
                let calls: Vec<Call> = calls.read().await.values().cloned().collect();
                for call in &calls {
                    for ev in &resp.to_device {
                        call.handle_to_device(ev).await;
                    }
                    if let Err(e) = call.refresh_members().await {
                        tracing::warn!(user = %who, room = %call.room_id(), "refreshing call members: {e}");
                    }
                }
                if to_device > 0 {
                    let dir = store_dir.clone();
                    match tokio::task::spawn_blocking(move || purge_room_keys(&dir)).await {
                        Ok(Ok(0)) => {}
                        Ok(Ok(n)) => tracing::info!(user = %who, discarded = n, "room keys discarded"),
                        Ok(Err(e)) => tracing::warn!(user = %who, "discarding room keys: {e}"),
                        Err(e) => tracing::warn!(user = %who, "discarding room keys: {e}"),
                    }
                }
                tracing::debug!(user = %who, to_device, timeline_events, ?timeline_types, "synced");
            }
            Err(e) => {
                failures += 1;
                let backoff = Duration::from_secs(u64::from(failures.min(6)) * 5);
                tracing::warn!(user = %who, failures, "sync failed: {e}");
                let mut stop = stop.clone();
                tokio::select! {
                    _ = tokio::time::sleep(backoff) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }
    tracing::info!(user = %who, "sync stopped");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn rooms() -> Vec<OwnedRoomId> {
        vec!["!a:s".try_into().unwrap(), "!b:s".try_into().unwrap()]
    }

    #[test]
    fn sync_filter_lists_every_call_room() {
        let v = serde_json::to_value(sync_filter(&rooms())).unwrap();
        assert_eq!(v["room"]["rooms"], json!(["!a:s", "!b:s"]));
    }

    #[test]
    fn sync_filter_has_no_timeline_and_only_call_state() {
        let v: Value = serde_json::to_value(sync_filter(&rooms())).unwrap();
        assert_eq!(v["room"]["timeline"]["limit"], 0);
        assert_eq!(v["room"]["timeline"]["types"], json!(SYNCED_STATE));
        assert_eq!(
            v["room"]["state"]["types"],
            json!(["org.matrix.msc3401.call.member", "m.room.member"])
        );
        assert!(v["room"]["state"].get("lazy_load_members").is_none());
        assert_eq!(v["room"]["ephemeral"]["types"], json!([]));
        assert_eq!(v["room"]["account_data"]["types"], json!([]));
        assert_eq!(v["presence"]["types"], json!([]));
        assert_eq!(v["account_data"]["types"], json!([]));
        assert!(v["room"].get("include_leave").is_none_or(|l| l == false));
    }

    #[test]
    fn store_dir_is_per_device_and_path_safe() {
        let base = Path::new("/state");
        let a = store_dir(base, <&UserId>::try_from("@agent_x:s").unwrap(), "SUPERDIAL".into());
        let b = store_dir(base, <&UserId>::try_from("@agent_y:s").unwrap(), "SUPERDIAL".into());
        let c = store_dir(base, <&UserId>::try_from("@agent_x:s").unwrap(), "OTHER".into());
        assert_ne!(a, b);
        assert_ne!(a, c);
        for p in [&a, &b, &c] {
            assert_eq!(p.parent().unwrap(), Path::new("/state/devices"));
        }
        let evil = store_dir(base, <&UserId>::try_from("@x:s").unwrap(), "../../etc".into());
        assert_eq!(evil.parent().unwrap(), Path::new("/state/devices"));
        assert!(!evil.file_name().unwrap().to_str().unwrap().contains('/'));
        assert!(!evil.file_name().unwrap().to_str().unwrap().contains(".."));
    }

    #[test]
    fn purge_room_keys_empties_table() {
        let dir = tempfile::tempdir().unwrap();
        let db = rusqlite::Connection::open(dir.path().join(CRYPTO_DB)).unwrap();
        db.execute_batch(
            "CREATE TABLE inbound_group_session (session_id TEXT PRIMARY KEY, data BLOB);
             INSERT INTO inbound_group_session VALUES ('a', x'00'), ('b', x'01');
             CREATE TABLE session (id TEXT); INSERT INTO session VALUES ('olm');",
        )
        .unwrap();
        assert_eq!(purge_room_keys(dir.path()).unwrap(), 2);
        let left: i64 = db
            .query_row("SELECT count(*) FROM inbound_group_session", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 0);
        let olm: i64 = db.query_row("SELECT count(*) FROM session", [], |r| r.get(0)).unwrap();
        assert_eq!(olm, 1, "Olm sessions are kept");
        assert_eq!(purge_room_keys(dir.path()).unwrap(), 0);
    }

    #[test]
    fn purge_room_keys_without_db_is_zero() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(purge_room_keys(dir.path()).unwrap(), 0);
        assert!(!dir.path().join(CRYPTO_DB).exists(), "does not create the store");
    }
}
