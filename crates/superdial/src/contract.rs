//! Wire names and message shapes fixed by the superdial contracts (C1, C2, C3, C5).

use std::time::Duration;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

/// Legacy MatrixRTC membership state event type (protocol.md §3).
pub const CALL_MEMBER_EVENT: &str = "org.matrix.msc3401.call.member";
/// C1: the call key, sent Olm-encrypted to one device.
pub const KEYS_EVENT: &str = "dev.superdial.call.encryption_keys";
/// C2: the transcript of one spoken turn, sent Olm-encrypted to the caller's call device.
pub const TRANSCRIPT_EVENT: &str = "dev.superdial.call.transcript";
/// C3: the field marking a posted `m.room.message` as a spoken turn.
pub const SPOKEN_FIELD: &str = "dev.superdial.spoken";
/// HKDF salt of LiveKit frame encryption.
pub const FRAME_KEY_SALT: &str = "LKFrameEncryptionKey";
/// The delayed leave armed before publishing membership, in milliseconds.
pub const DELAYED_LEAVE_MS: u64 = 8000;
/// How often the delayed leave is restarted.
pub const RESTART_EVERY: Duration = Duration::from_secs(5);
/// `expires` of a published membership, in milliseconds (4 h).
pub const MEMBERSHIP_EXPIRES_MS: u64 = 14_400_000;

/// C1 content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallKeyContent {
    pub room_id: String,
    pub member_id: String,
    pub member: KeyMember,
    pub keys: KeyEntry,
    pub sent_ts: u64,
}

/// The sender named in a C1 message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyMember {
    pub user_id: String,
    pub device_id: String,
}

/// One media key in a C1 message: `key` is unpadded base64 of 16 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyEntry {
    pub index: u32,
    pub key: String,
}

/// C2 content. `lang` is always present, `null` when unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TranscriptContent {
    pub room_id: String,
    pub turn_id: String,
    pub text: String,
    pub lang: Option<String>,
    pub audio_ms: u64,
    pub sent_ts: u64,
}

/// C3 marker carried under [`SPOKEN_FIELD`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpokenMarker {
    pub turn_id: String,
    pub agent_user_id: String,
    pub audio_ms: u64,
}

/// C3: the `m.room.message` content a caller posts for a transcript.
pub fn spoken_message_content(text: &str, marker: &SpokenMarker) -> Value {
    json!({"msgtype": "m.text", "body": text, SPOKEN_FIELD: marker})
}

/// The membership state key: `_{user_id}_{device_id}_m.call`.
pub fn membership_state_key(user_id: &str, device_id: &str) -> String {
    format!("_{user_id}_{device_id}_m.call")
}

/// The LiveKit participant identity (C5): `{user_id}:{device_id}`. The only form keys are
/// registered under.
pub fn livekit_identity(user_id: &str, device_id: &str) -> String {
    format!("{user_id}:{device_id}")
}

/// The LiveKit room name (C5): unpadded base64url of `sha256(room_id)`.
pub fn livekit_room_name(room_id: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(room_id.as_bytes()))
}

/// Membership content, exactly the fields of protocol.md §3.
pub fn membership_content(device_id: &str, membership_id: &str, livekit_service_url: &str, room_id: &str) -> Value {
    json!({
        "application": "m.call",
        "call_id": "",
        "scope": "m.room",
        "device_id": device_id,
        "membershipID": membership_id,
        "expires": MEMBERSHIP_EXPIRES_MS,
        "focus_active": {"type": "livekit", "focus_selection": "oldest_membership"},
        "foci_preferred": [{"type": "livekit", "livekit_service_url": livekit_service_url, "livekit_alias": room_id}],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn fixture(name: &str) -> Value {
        let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    const TURN: &str = "0b6c7c4e-3f0e-4c1b-9a43-6f1d2a9e5b77";

    fn c1() -> CallKeyContent {
        CallKeyContent {
            room_id: "!r:s".into(),
            member_id: "@u:s:DEV".into(),
            member: KeyMember {
                user_id: "@u:s".into(),
                device_id: "DEV".into(),
            },
            keys: KeyEntry {
                index: 0,
                key: "AAECAwQFBgcICQoLDA0ODw".into(),
            },
            sent_ts: 1_764_200_000_000,
        }
    }

    #[test]
    fn c1_serializes_to_contract_fixture() {
        assert_eq!(serde_json::to_value(c1()).unwrap(), fixture("c1.json"));
    }

    #[test]
    fn c1_fixture_round_trips() {
        let parsed: CallKeyContent = serde_json::from_value(fixture("c1.json")).unwrap();
        assert_eq!(parsed, c1());
    }

    fn c2(lang: Option<&str>) -> TranscriptContent {
        TranscriptContent {
            room_id: "!r:s".into(),
            turn_id: TURN.into(),
            text: "what the person said".into(),
            lang: lang.map(str::to_owned),
            audio_ms: 2310,
            sent_ts: 1_764_200_000_000,
        }
    }

    #[test]
    fn c2_serializes_to_contract_fixture() {
        assert_eq!(serde_json::to_value(c2(Some("en"))).unwrap(), fixture("c2.json"));
        let parsed: TranscriptContent = serde_json::from_value(fixture("c2.json")).unwrap();
        assert_eq!(parsed, c2(Some("en")));
    }

    #[test]
    fn c2_lang_null_is_present() {
        let v = serde_json::to_value(c2(None)).unwrap();
        assert!(v.as_object().unwrap().contains_key("lang"));
        assert_eq!(v["lang"], Value::Null);
    }

    #[test]
    fn c3_content_matches_fixture() {
        let marker = SpokenMarker {
            turn_id: TURN.into(),
            agent_user_id: "@agent_x:s".into(),
            audio_ms: 2310,
        };
        assert_eq!(
            spoken_message_content("what the person said", &marker),
            fixture("c3.json")
        );
    }

    #[test]
    fn state_key_form() {
        assert_eq!(membership_state_key("@u:s", "DEV"), "_@u:s_DEV_m.call");
    }

    #[test]
    fn identity_form() {
        assert_eq!(livekit_identity("@u:s", "DEV"), "@u:s:DEV");
    }

    #[test]
    fn room_name_is_base64url_nopad_sha256() {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(b"!r:s");
        // Independent unpadded base64url encoding of the 32 digest bytes.
        const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
        let mut bits = String::new();
        for b in digest.iter() {
            bits.push_str(&format!("{b:08b}"));
        }
        while bits.len() % 6 != 0 {
            bits.push('0');
        }
        let expected: String = bits
            .as_bytes()
            .chunks(6)
            .map(|c| ALPHABET[usize::from_str_radix(std::str::from_utf8(c).unwrap(), 2).unwrap()] as char)
            .collect();
        let name = livekit_room_name("!r:s");
        assert_eq!(name, expected);
        assert_eq!(name.len(), 43);
        assert!(!name.contains('=') && !name.contains('+') && !name.contains('/'));
    }

    #[test]
    fn membership_content_is_exactly_protocol_fields() {
        let v = membership_content("DEV", "m-1", "https://lk.example", "!r:s");
        let mut keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "application",
                "call_id",
                "device_id",
                "expires",
                "foci_preferred",
                "focus_active",
                "membershipID",
                "scope"
            ]
        );
        assert_eq!(v["application"], "m.call");
        assert_eq!(v["call_id"], "");
        assert_eq!(v["scope"], "m.room");
        assert_eq!(v["device_id"], "DEV");
        assert_eq!(v["membershipID"], "m-1");
        assert_eq!(v["expires"], 14_400_000);
        assert_eq!(
            v["focus_active"],
            json!({"type": "livekit", "focus_selection": "oldest_membership"})
        );
        assert_eq!(
            v["foci_preferred"],
            json!([{"type": "livekit", "livekit_service_url": "https://lk.example", "livekit_alias": "!r:s"}])
        );
    }

    #[test]
    fn event_type_constants() {
        assert_eq!(CALL_MEMBER_EVENT, "org.matrix.msc3401.call.member");
        assert_eq!(KEYS_EVENT, "dev.superdial.call.encryption_keys");
        assert_eq!(TRANSCRIPT_EVENT, "dev.superdial.call.transcript");
        assert_eq!(SPOKEN_FIELD, "dev.superdial.spoken");
        assert_eq!(FRAME_KEY_SALT, "LKFrameEncryptionKey");
        assert_eq!(DELAYED_LEAVE_MS, 8000);
        assert_eq!(RESTART_EVERY, std::time::Duration::from_secs(5));
    }
}
