//! Pure bookkeeping for one call: who is in it, who has our key, which peer keys apply, and
//! which transcripts to accept.

use std::collections::{HashMap, HashSet};

use matrix_sdk::ruma::{DeviceId, OwnedDeviceId, OwnedRoomId, OwnedUserId, UserId};

use crate::{contract::TranscriptContent, keys::AcceptedKey, membership::CallMember};

/// What changed in a membership update, and what to do about it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MemberDiff {
    pub joined: Vec<CallMember>,
    pub left: Vec<CallMember>,
    /// Members that have not been sent our key yet.
    pub to_key: Vec<CallMember>,
    /// Buffered keys whose sender is now a member.
    pub keys_to_apply: Vec<AcceptedKey>,
}

/// Why a C2 transcript was not accepted.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TranscriptRejection {
    #[error("transcript is not from the expected agent")]
    NotExpected,
    #[error("transcript is from a device not in the call")]
    NotMember,
    #[error("transcript is for another room")]
    WrongRoom,
    #[error("transcript was not encrypted")]
    Unencrypted,
    #[error("malformed transcript: {0}")]
    Malformed(String),
}

type DeviceKey = (OwnedUserId, OwnedDeviceId);

/// The state of one call from one participant's side. No I/O.
#[derive(Debug)]
pub struct CallState {
    room_id: OwnedRoomId,
    own_user: OwnedUserId,
    own_device: OwnedDeviceId,
    transcripts_from: Option<OwnedUserId>,
    members: Vec<CallMember>,
    keyed: HashSet<CallMember>,
    pending_keys: HashMap<DeviceKey, AcceptedKey>,
}

impl CallState {
    pub fn new(
        room_id: OwnedRoomId,
        own_user: OwnedUserId,
        own_device: OwnedDeviceId,
        transcripts_from: Option<OwnedUserId>,
    ) -> Self {
        Self {
            room_id,
            own_user,
            own_device,
            transcripts_from,
            members: Vec::new(),
            keyed: HashSet::new(),
            pending_keys: HashMap::new(),
        }
    }

    pub fn room_id(&self) -> &OwnedRoomId {
        &self.room_id
    }

    /// The other participants' devices.
    pub fn members(&self) -> &[CallMember] {
        &self.members
    }

    pub fn is_member(&self, user: &UserId, device: &DeviceId) -> bool {
        self.members.iter().any(|m| m.user_id == user && m.device_id == device)
    }

    /// Replaces the member list with `all` (as read from room state). Our own device is left
    /// out. A device whose membership ID changed has left and joined again.
    pub fn update_members(&mut self, all: Vec<CallMember>) -> MemberDiff {
        let mut next: Vec<CallMember> = Vec::new();
        for m in all {
            let own = m.user_id == self.own_user && m.device_id == self.own_device;
            if !own && !next.contains(&m) {
                next.push(m);
            }
        }
        let mut diff = MemberDiff::default();
        for old in &self.members {
            if !next.contains(old) {
                diff.left.push(old.clone());
                self.keyed.remove(old);
            }
        }
        for m in &next {
            if !self.members.contains(m) {
                diff.joined.push(m.clone());
            }
            if self.keyed.insert(m.clone()) {
                diff.to_key.push(m.clone());
            }
            if let Some(k) = self.pending_keys.remove(&(m.user_id.clone(), m.device_id.clone())) {
                diff.keys_to_apply.push(k);
            }
        }
        self.members = next;
        diff
    }

    /// A peer key arrived. Returns it if its device is in the call now; otherwise keeps the
    /// latest one per device until that device's membership appears.
    pub fn on_key(&mut self, key: AcceptedKey) -> Option<AcceptedKey> {
        if self.is_member(&key.user_id, &key.device_id) {
            return Some(key);
        }
        self.pending_keys
            .insert((key.user_id.clone(), key.device_id.clone()), key);
        None
    }

    /// Accepts a C2 transcript only from the expected agent's device that is in this call.
    pub fn accept_transcript(
        &self,
        content: &TranscriptContent,
        sender: &UserId,
        sender_device: &DeviceId,
    ) -> Result<CallMember, TranscriptRejection> {
        if content.room_id != self.room_id.as_str() {
            return Err(TranscriptRejection::WrongRoom);
        }
        if self.transcripts_from.as_deref() != Some(sender) {
            return Err(TranscriptRejection::NotExpected);
        }
        self.members
            .iter()
            .find(|m| m.user_id == sender && m.device_id == sender_device)
            .cloned()
            .ok_or(TranscriptRejection::NotMember)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys::CallKey;
    use matrix_sdk::ruma::{device_id, owned_room_id, owned_user_id, user_id};

    fn member(user: &str, device: &str, mid: &str) -> CallMember {
        CallMember {
            user_id: user.try_into().unwrap(),
            device_id: device.into(),
            membership_id: mid.into(),
        }
    }

    fn me() -> CallMember {
        member("@me:s", "MINE", "m-me")
    }

    fn agent() -> CallMember {
        member("@agent:s", "SUPERDIAL", "m-agent")
    }

    fn state(transcripts_from: Option<&str>) -> CallState {
        CallState::new(
            owned_room_id!("!r:s"),
            owned_user_id!("@me:s"),
            "MINE".into(),
            transcripts_from.map(|u| u.try_into().unwrap()),
        )
    }

    fn key_from(m: &CallMember, byte: u8) -> AcceptedKey {
        AcceptedKey {
            identity: m.identity(),
            user_id: m.user_id.clone(),
            device_id: m.device_id.clone(),
            index: 0,
            key: CallKey::from_bytes([byte; 16]),
        }
    }

    fn transcript(room: &str) -> TranscriptContent {
        TranscriptContent {
            room_id: room.into(),
            turn_id: "t".into(),
            text: "hi".into(),
            lang: None,
            audio_ms: 1,
            sent_ts: 1,
        }
    }

    #[test]
    fn own_device_is_not_a_peer() {
        let mut s = state(None);
        let d = s.update_members(vec![me()]);
        assert!(d.joined.is_empty() && d.to_key.is_empty());
        assert!(s.members().is_empty());
    }

    #[test]
    fn own_user_other_device_is_a_peer() {
        let mut s = state(None);
        let d = s.update_members(vec![member("@me:s", "PHONE", "m-2")]);
        assert_eq!(d.joined.len(), 1);
    }

    #[test]
    fn new_member_is_to_key_once() {
        let mut s = state(None);
        let d = s.update_members(vec![me(), agent()]);
        assert_eq!(d.joined, vec![agent()]);
        assert_eq!(d.to_key, vec![agent()]);
        let d = s.update_members(vec![me(), agent()]);
        assert!(d.joined.is_empty() && d.to_key.is_empty() && d.left.is_empty());
    }

    #[test]
    fn key_before_membership_is_applied_when_member_appears() {
        let mut s = state(None);
        assert_eq!(s.on_key(key_from(&agent(), 1)), None);
        let d = s.update_members(vec![agent()]);
        assert_eq!(d.keys_to_apply, vec![key_from(&agent(), 1)]);
        // Released once only.
        assert!(s.update_members(vec![agent()]).keys_to_apply.is_empty());
    }

    #[test]
    fn latest_buffered_key_wins() {
        let mut s = state(None);
        s.on_key(key_from(&agent(), 1));
        s.on_key(key_from(&agent(), 2));
        assert_eq!(
            s.update_members(vec![agent()]).keys_to_apply,
            vec![key_from(&agent(), 2)]
        );
    }

    #[test]
    fn key_from_current_member_applies_now() {
        let mut s = state(None);
        s.update_members(vec![agent()]);
        assert_eq!(s.on_key(key_from(&agent(), 3)), Some(key_from(&agent(), 3)));
    }

    #[test]
    fn left_member_reported_and_forgotten() {
        let mut s = state(None);
        s.update_members(vec![agent()]);
        let d = s.update_members(vec![]);
        assert_eq!(d.left, vec![agent()]);
        assert!(!s.is_member(user_id!("@agent:s"), device_id!("SUPERDIAL")));
        // A key arriving after the leave is buffered, not applied.
        assert_eq!(s.on_key(key_from(&agent(), 4)), None);
    }

    #[test]
    fn rejoined_member_is_keyed_again() {
        let mut s = state(None);
        s.update_members(vec![agent()]);
        s.update_members(vec![]);
        let d = s.update_members(vec![agent()]);
        assert_eq!(d.joined, vec![agent()]);
        assert_eq!(d.to_key, vec![agent()]);
    }

    #[test]
    fn new_membership_id_on_same_device_is_a_rejoin() {
        let mut s = state(None);
        s.update_members(vec![agent()]);
        let again = member("@agent:s", "SUPERDIAL", "m-agent-2");
        let d = s.update_members(vec![again.clone()]);
        assert_eq!(d.left, vec![agent()]);
        assert_eq!(d.joined, vec![again.clone()]);
        assert_eq!(d.to_key, vec![again]);
    }

    #[test]
    fn transcript_from_agent_member_accepted() {
        let mut s = state(Some("@agent:s"));
        s.update_members(vec![agent()]);
        assert_eq!(
            s.accept_transcript(&transcript("!r:s"), user_id!("@agent:s"), device_id!("SUPERDIAL")),
            Ok(agent())
        );
    }

    #[test]
    fn transcript_from_other_user_rejected() {
        let mut s = state(Some("@agent:s"));
        let other = member("@other:s", "X", "m-x");
        s.update_members(vec![agent(), other]);
        assert_eq!(
            s.accept_transcript(&transcript("!r:s"), user_id!("@other:s"), device_id!("X")),
            Err(TranscriptRejection::NotExpected)
        );
    }

    #[test]
    fn transcript_from_agent_non_call_device_rejected() {
        let mut s = state(Some("@agent:s"));
        s.update_members(vec![agent()]);
        assert_eq!(
            s.accept_transcript(&transcript("!r:s"), user_id!("@agent:s"), device_id!("HUBDEVICE")),
            Err(TranscriptRejection::NotMember)
        );
    }

    #[test]
    fn transcript_without_expected_agent_rejected() {
        let mut s = state(None);
        s.update_members(vec![agent()]);
        assert_eq!(
            s.accept_transcript(&transcript("!r:s"), user_id!("@agent:s"), device_id!("SUPERDIAL")),
            Err(TranscriptRejection::NotExpected)
        );
    }

    #[test]
    fn transcript_for_other_room_rejected() {
        let mut s = state(Some("@agent:s"));
        s.update_members(vec![agent()]);
        assert_eq!(
            s.accept_transcript(&transcript("!x:s"), user_id!("@agent:s"), device_id!("SUPERDIAL")),
            Err(TranscriptRejection::WrongRoom)
        );
    }
}
