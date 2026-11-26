//! superdial: voice calls over MatrixRTC and LiveKit, end-to-end encrypted.
//!
//! The library holds the call protocol only: no UI and no platform audio code.

pub mod contract;
pub mod delayed_leave;
pub mod error;
pub mod homeserver;
pub mod issuer;
pub mod keys;
pub mod membership;
pub mod state;

pub use error::Error;
