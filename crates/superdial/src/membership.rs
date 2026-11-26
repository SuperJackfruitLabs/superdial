//! Active call memberships, read from `org.matrix.msc3401.call.member` state.

use matrix_sdk::ruma::{OwnedDeviceId, OwnedUserId};
use serde_json::Value;

use crate::contract::{livekit_identity, membership_state_key, MEMBERSHIP_EXPIRES_MS};

/// One device in a call.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CallMember {
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
    pub membership_id: String,
}

impl CallMember {
    /// The member's LiveKit identity, `{user_id}:{device_id}`.
    pub fn identity(&self) -> String {
        livekit_identity(self.user_id.as_str(), self.device_id.as_str())
    }
}

/// The active memberships among raw state events (each with `sender`, `state_key`, `content`,
/// `origin_server_ts`). A leave is an empty content. A membership counts only if its state key
/// is the sender's own (`_{sender}_{device_id}_m.call`), its application is `m.call`, and it has
/// not expired.
pub fn active_members(events: &[Value], now_ms: u64) -> Vec<CallMember> {
    events.iter().filter_map(|e| active_member(e, now_ms)).collect()
}

fn active_member(e: &Value, now_ms: u64) -> Option<CallMember> {
    let sender = e["sender"].as_str()?;
    let c = &e["content"];
    if c["application"] != "m.call" {
        return None;
    }
    let device = c["device_id"].as_str()?;
    if e["state_key"].as_str()? != membership_state_key(sender, device) {
        return None;
    }
    let sent = e["origin_server_ts"].as_u64().unwrap_or(0);
    let expires = c["expires"].as_u64().unwrap_or(MEMBERSHIP_EXPIRES_MS);
    if sent.saturating_add(expires) < now_ms {
        return None;
    }
    let user_id = OwnedUserId::try_from(sender).ok()?;
    let membership_id = c["membershipID"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| livekit_identity(sender, device));
    Some(CallMember {
        user_id,
        device_id: device.into(),
        membership_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::membership_content;
    use serde_json::{json, Value};

    const NOW: u64 = 1_764_200_000_000;

    fn ev(sender: &str, device: &str, content: Value, ts: u64) -> Value {
        json!({
            "type": "org.matrix.msc3401.call.member",
            "sender": sender,
            "state_key": format!("_{sender}_{device}_m.call"),
            "content": content,
            "origin_server_ts": ts,
            "event_id": "$e",
        })
    }

    fn joined(sender: &str, device: &str) -> Value {
        ev(
            sender,
            device,
            membership_content(device, "m-1", "https://lk", "!r:s"),
            NOW - 1000,
        )
    }

    #[test]
    fn parses_protocol_membership() {
        let m = active_members(&[joined("@u:s", "DEV")], NOW);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].user_id, "@u:s");
        assert_eq!(m[0].device_id, "DEV");
        assert_eq!(m[0].membership_id, "m-1");
        assert_eq!(m[0].identity(), "@u:s:DEV");
    }

    #[test]
    fn ignores_empty_content() {
        assert!(active_members(&[ev("@u:s", "DEV", json!({}), NOW)], NOW).is_empty());
    }

    #[test]
    fn leave_with_prev_content_is_not_active() {
        // What tuwunel 2.0.0 sends for a leave, including a delayed leave it fired itself.
        let mut e = ev("@u:s", "DEV", json!({}), NOW);
        e["unsigned"] = json!({"prev_content": membership_content("DEV", "m-1", "https://lk", "!r:s")});
        assert!(active_members(&[e], NOW).is_empty());
    }

    #[test]
    fn ignores_other_application() {
        let mut c = membership_content("DEV", "m-1", "https://lk", "!r:s");
        c["application"] = json!("m.other");
        assert!(active_members(&[ev("@u:s", "DEV", c, NOW)], NOW).is_empty());
    }

    #[test]
    fn ignores_expired() {
        let c = membership_content("DEV", "m-1", "https://lk", "!r:s");
        let old = ev("@u:s", "DEV", c.clone(), NOW - 14_400_001);
        assert!(active_members(&[old], NOW).is_empty());
        let fresh = ev("@u:s", "DEV", c, NOW - 14_399_000);
        assert_eq!(active_members(&[fresh], NOW).len(), 1);
    }

    #[test]
    fn ignores_state_key_not_matching_sender() {
        let mut e = joined("@mallory:s", "DEV");
        e["state_key"] = json!("_@victim:s_DEV_m.call");
        assert!(active_members(&[e], NOW).is_empty());
        let mut e = joined("@u:s", "DEV");
        e["content"]["device_id"] = json!("OTHER");
        assert!(active_members(&[e], NOW).is_empty());
    }

    #[test]
    fn missing_membership_id_falls_back_to_identity() {
        let mut e = joined("@u:s", "DEV");
        e["content"].as_object_mut().unwrap().remove("membershipID");
        assert_eq!(active_members(&[e], NOW)[0].membership_id, "@u:s:DEV");
    }

    #[test]
    fn invalid_user_id_is_skipped() {
        let e = joined("not-a-user", "DEV");
        assert!(active_members(&[e], NOW).is_empty());
    }
}
