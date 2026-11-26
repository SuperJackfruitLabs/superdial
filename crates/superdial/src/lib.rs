//! superdial: voice calls over MatrixRTC and LiveKit, end-to-end encrypted.
//!
//! The library holds the call protocol only: no UI and no platform audio code. The
//! application owns the matrix-sdk [`matrix_sdk::Client`] and its sync loop; it joins with
//! [`Call::join`], and after each sync calls [`Call::refresh_members`] and passes to-device
//! events to [`Call::handle_to_device`].

pub mod call;
pub mod contract;
pub mod delayed_leave;
pub mod error;
pub mod homeserver;
pub mod issuer;
pub mod keys;
pub mod media;
pub mod membership;
pub mod state;

pub use call::{post_spoken_turn, Call, CallEvent, CallOptions};
pub use contract::{
    livekit_identity, livekit_room_name, membership_content, membership_state_key, spoken_message_content,
    CallKeyContent, SpokenMarker, TranscriptContent, CALL_MEMBER_EVENT, KEYS_EVENT, SPOKEN_FIELD, TRANSCRIPT_EVENT,
};
pub use error::Error;
pub use homeserver::Homeserver;
#[cfg(feature = "dev-lk-jwt")]
pub use issuer::LkJwtDevIssuer;
pub use issuer::{IssuerError, LiveKitGrant, RefusalReason, SuperExchangeIssuer, TokenIssuer};
pub use keys::CallKey;
pub use media::{E2eeState, Media, MediaEvent, SAMPLE_RATE};
pub use membership::{active_members, CallMember};
