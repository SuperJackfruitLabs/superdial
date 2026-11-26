//! Media keys: generation, encoding, and the C1 message rules.

use base64::{
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
    Engine,
};
use matrix_sdk::ruma::{DeviceId, OwnedDeviceId, OwnedUserId, RoomId, UserId};
use serde_json::Value;

use crate::contract::{livekit_identity, CallKeyContent, KeyEntry, KeyMember};

/// Length of a call key in bytes (C1).
pub const KEY_LEN: usize = 16;

/// One participant's media key for a call.
#[derive(Clone, PartialEq, Eq)]
pub struct CallKey([u8; KEY_LEN]);

impl std::fmt::Debug for CallKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CallKey(..)")
    }
}

/// Why a key string is not a call key.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("key is not base64")]
    Encoding,
    #[error("key is {0} bytes, expected 16")]
    Length(usize),
}

impl CallKey {
    /// A new random key.
    pub fn generate() -> Self {
        Self(rand::random())
    }

    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; KEY_LEN] {
        &self.0
    }

    /// Unpadded standard base64, as C1 carries it.
    pub fn to_base64(&self) -> String {
        STANDARD_NO_PAD.encode(self.0)
    }

    /// Accepts unpadded (the contract form) or padded base64; the key must be 16 bytes.
    pub fn from_base64(s: &str) -> Result<Self, KeyError> {
        let bytes = STANDARD_NO_PAD
            .decode(s)
            .or_else(|_| STANDARD.decode(s))
            .map_err(|_| KeyError::Encoding)?;
        let arr: [u8; KEY_LEN] = bytes.as_slice().try_into().map_err(|_| KeyError::Length(bytes.len()))?;
        Ok(Self(arr))
    }
}

/// The C1 content announcing our key.
pub fn key_message(
    room_id: &str,
    user_id: &str,
    device_id: &str,
    member_id: &str,
    index: u32,
    key: &CallKey,
    now_ms: u64,
) -> CallKeyContent {
    CallKeyContent {
        room_id: room_id.to_owned(),
        member_id: member_id.to_owned(),
        member: KeyMember {
            user_id: user_id.to_owned(),
            device_id: device_id.to_owned(),
        },
        keys: KeyEntry {
            index,
            key: key.to_base64(),
        },
        sent_ts: now_ms,
    }
}

/// A peer's key, ready to register under its LiveKit identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptedKey {
    pub identity: String,
    pub user_id: OwnedUserId,
    pub device_id: OwnedDeviceId,
    pub index: u32,
    pub key: CallKey,
}

/// Why a C1 message was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyRejection {
    #[error("malformed call key message: {0}")]
    Malformed(String),
    #[error("call key is for another room")]
    WrongRoom,
    #[error("call key names a member other than its Olm sender")]
    MemberMismatch,
    #[error("bad call key: {0}")]
    BadKey(KeyError),
    #[error("call key from a device not in the call")]
    NotMember,
    #[error("call key was not encrypted")]
    Unencrypted,
}

/// Checks a decrypted C1 message. The identity is taken from the Olm sender and device, never
/// from the content; whether that device is in the call is [`crate::state::CallState`]'s job.
pub fn parse_key_event(
    content: &Value,
    room_id: &RoomId,
    sender: &UserId,
    sender_device: &DeviceId,
) -> Result<AcceptedKey, KeyRejection> {
    let c: CallKeyContent =
        serde_json::from_value(content.clone()).map_err(|e| KeyRejection::Malformed(e.to_string()))?;
    if c.room_id != room_id.as_str() {
        return Err(KeyRejection::WrongRoom);
    }
    if c.member.user_id != sender.as_str() || c.member.device_id != sender_device.as_str() {
        return Err(KeyRejection::MemberMismatch);
    }
    let key = CallKey::from_base64(&c.keys.key).map_err(KeyRejection::BadKey)?;
    Ok(AcceptedKey {
        identity: livekit_identity(sender.as_str(), sender_device.as_str()),
        user_id: sender.to_owned(),
        device_id: sender_device.to_owned(),
        index: c.keys.index,
        key,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use matrix_sdk::ruma::{device_id, room_id, user_id};
    use serde_json::{json, Value};

    fn fixture(name: &str) -> Value {
        let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn counting_key() -> CallKey {
        CallKey::from_bytes(std::array::from_fn(|i| i as u8))
    }

    #[test]
    fn generated_keys_are_16_bytes_and_differ() {
        let (a, b) = (CallKey::generate(), CallKey::generate());
        assert_eq!(a.as_bytes().len(), 16);
        assert_ne!(a, b);
    }

    #[test]
    fn base64_is_unpadded_and_round_trips() {
        let k = counting_key();
        let s = k.to_base64();
        assert_eq!(s, "AAECAwQFBgcICQoLDA0ODw");
        assert_eq!(s.len(), 22);
        assert!(!s.contains('='));
        assert_eq!(CallKey::from_base64(&s).unwrap(), k);
    }

    #[test]
    fn accepts_padded_base64() {
        assert_eq!(
            CallKey::from_base64("AAECAwQFBgcICQoLDA0ODw==").unwrap(),
            counting_key()
        );
    }

    #[test]
    fn rejects_32_byte_key() {
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
        assert_eq!(CallKey::from_base64(s), Err(KeyError::Length(32)));
    }

    #[test]
    fn rejects_non_base64() {
        assert!(matches!(CallKey::from_base64("not base64!"), Err(KeyError::Encoding)));
    }

    #[test]
    fn key_message_matches_c1_shape() {
        let m = key_message("!r:s", "@u:s", "DEV", "@u:s:DEV", 0, &counting_key(), 1_764_200_000_000);
        assert_eq!(serde_json::to_value(m).unwrap(), fixture("c1.json"));
    }

    #[test]
    fn parse_registers_under_olm_sender_identity() {
        let k = parse_key_event(
            &fixture("c1.json"),
            room_id!("!r:s"),
            user_id!("@u:s"),
            device_id!("DEV"),
        )
        .unwrap();
        assert_eq!(k.identity, "@u:s:DEV");
        assert_eq!(k.user_id, "@u:s");
        assert_eq!(k.device_id, "DEV");
        assert_eq!(k.index, 0);
        assert_eq!(k.key, counting_key());
    }

    #[test]
    fn parse_rejects_member_mismatch() {
        let r = parse_key_event(
            &fixture("c1.json"),
            room_id!("!r:s"),
            user_id!("@u:s"),
            device_id!("OTHER"),
        );
        assert_eq!(r.unwrap_err(), KeyRejection::MemberMismatch);
        let r = parse_key_event(
            &fixture("c1.json"),
            room_id!("!r:s"),
            user_id!("@m:s"),
            device_id!("DEV"),
        );
        assert_eq!(r.unwrap_err(), KeyRejection::MemberMismatch);
    }

    #[test]
    fn parse_rejects_wrong_room() {
        let r = parse_key_event(
            &fixture("c1.json"),
            room_id!("!other:s"),
            user_id!("@u:s"),
            device_id!("DEV"),
        );
        assert_eq!(r.unwrap_err(), KeyRejection::WrongRoom);
    }

    #[test]
    fn parse_rejects_missing_keys() {
        let mut v = fixture("c1.json");
        v.as_object_mut().unwrap().remove("keys");
        let r = parse_key_event(&v, room_id!("!r:s"), user_id!("@u:s"), device_id!("DEV"));
        assert!(matches!(r.unwrap_err(), KeyRejection::Malformed(_)));
    }

    #[test]
    fn parse_rejects_bad_key_length() {
        let mut v = fixture("c1.json");
        v["keys"] = json!({"index": 0, "key": "AAEC"});
        let r = parse_key_event(&v, room_id!("!r:s"), user_id!("@u:s"), device_id!("DEV"));
        assert_eq!(r.unwrap_err(), KeyRejection::BadKey(KeyError::Length(3)));
    }
}
