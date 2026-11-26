//! The LiveKit side of a call: connect with end-to-end encryption, publish 48 kHz mono PCM,
//! and receive every remote audio track as 48 kHz mono PCM.

use std::{collections::HashMap, sync::Arc};

use futures::StreamExt;
pub use livekit::e2ee::key_provider::{KeyDerivationAlgorithm, KeyProviderOptions};
use livekit::{
    e2ee::{key_provider::KeyProvider, E2eeOptions, EncryptionType},
    options::TrackPublishOptions,
    prelude::*,
    webrtc::{
        audio_frame::AudioFrame,
        audio_source::{native::NativeAudioSource, AudioSourceOptions, RtcAudioSource},
        audio_stream::native::NativeAudioStream,
        native::frame_cryptor::EncryptionState,
    },
};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{contract::FRAME_KEY_SALT, issuer::LiveKitGrant, keys::CallKey, Error};

/// Audio crosses the library boundary at this rate, mono, i16.
pub const SAMPLE_RATE: u32 = 48_000;
/// Samples in one 10 ms frame at [`SAMPLE_RATE`].
pub const FRAME_SAMPLES: usize = 480;

/// LiveKit key provider options for MatrixRTC media: HKDF with salt `LKFrameEncryptionKey`,
/// ratchet window 0.
pub fn key_provider_options() -> KeyProviderOptions {
    KeyProviderOptions {
        ratchet_window_size: 0,
        ratchet_salt: FRAME_KEY_SALT.as_bytes().to_vec(),
        failure_tolerance: 10,
        key_ring_size: 255,
        key_derivation_algorithm: KeyDerivationAlgorithm::HKDF,
    }
}

/// Splits PCM into 10 ms frames, zero-padding the last.
pub fn ten_ms_frames(samples: &[i16]) -> Vec<Vec<i16>> {
    samples
        .chunks(FRAME_SAMPLES)
        .map(|c| {
            let mut f = c.to_vec();
            f.resize(FRAME_SAMPLES, 0);
            f
        })
        .collect()
}

/// A participant's frame encryption state, as LiveKit reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum E2eeState {
    New,
    Ok,
    EncryptionFailed,
    DecryptionFailed,
    MissingKey,
    KeyRatcheted,
    InternalError,
}

impl E2eeState {
    pub fn as_str(self) -> &'static str {
        match self {
            E2eeState::New => "New",
            E2eeState::Ok => "Ok",
            E2eeState::EncryptionFailed => "EncryptionFailed",
            E2eeState::DecryptionFailed => "DecryptionFailed",
            E2eeState::MissingKey => "MissingKey",
            E2eeState::KeyRatcheted => "KeyRatcheted",
            E2eeState::InternalError => "InternalError",
        }
    }
}

impl From<EncryptionState> for E2eeState {
    fn from(s: EncryptionState) -> Self {
        match s {
            EncryptionState::New => E2eeState::New,
            EncryptionState::Ok => E2eeState::Ok,
            EncryptionState::EncryptionFailed => E2eeState::EncryptionFailed,
            EncryptionState::DecryptionFailed => E2eeState::DecryptionFailed,
            EncryptionState::MissingKey => E2eeState::MissingKey,
            EncryptionState::KeyRatcheted => E2eeState::KeyRatcheted,
            EncryptionState::InternalError => E2eeState::InternalError,
        }
    }
}

/// What happens on the LiveKit side.
#[derive(Debug, Clone, PartialEq)]
pub enum MediaEvent {
    ParticipantConnected {
        identity: String,
    },
    ParticipantLeft {
        identity: String,
    },
    /// Frame encryption state for a participant (ours included).
    E2ee {
        identity: String,
        state: E2eeState,
    },
    /// 10 ms of 48 kHz mono audio from a remote participant, decrypted.
    Audio {
        identity: String,
        samples: Vec<i16>,
    },
    AttributesChanged {
        identity: String,
        attributes: HashMap<String, String>,
    },
    Disconnected {
        reason: String,
    },
}

/// A LiveKit connection with end-to-end encryption on and one published microphone track.
pub struct Media {
    room: Arc<Room>,
    keys: KeyProvider,
    source: NativeAudioSource,
    identity: String,
    forwarder: JoinHandle<()>,
}

impl Media {
    /// Connects with E2EE (AES-GCM, per-participant keys), registers our key under
    /// `grant.identity`, publishes a 48 kHz mono microphone track and subscribes to every
    /// remote audio track.
    pub async fn connect(
        grant: &LiveKitGrant,
        own_key: &CallKey,
    ) -> Result<(Media, mpsc::UnboundedReceiver<MediaEvent>), Error> {
        let keys = KeyProvider::new(key_provider_options());
        keys.set_key(
            &ParticipantIdentity(grant.identity.clone()),
            0,
            own_key.as_bytes().to_vec(),
        );
        let mut options = RoomOptions::default();
        options.auto_subscribe = true;
        options.encryption = Some(E2eeOptions {
            encryption_type: EncryptionType::Gcm,
            key_provider: keys.clone(),
        });
        let (room, events) = Room::connect(&grant.url, &grant.jwt, options)
            .await
            .map_err(|e| Error::LiveKit(e.to_string()))?;
        room.e2ee_manager().set_enabled(true);
        let room = Arc::new(room);

        let source = NativeAudioSource::new(AudioSourceOptions::default(), SAMPLE_RATE, 1, 1000);
        let track = LocalAudioTrack::create_audio_track("superdial-audio", RtcAudioSource::Native(source.clone()));
        room.local_participant()
            .publish_track(
                LocalTrack::Audio(track),
                TrackPublishOptions {
                    source: TrackSource::Microphone,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| Error::LiveKit(e.to_string()))?;

        let (tx, rx) = mpsc::unbounded_channel();
        let forwarder = tokio::spawn(forward_events(events, tx));
        let identity = room.local_participant().identity().to_string();
        Ok((
            Media {
                room,
                keys,
                source,
                identity,
                forwarder,
            },
            rx,
        ))
    }

    /// Registers a peer's key under its LiveKit identity.
    pub fn set_participant_key(&self, identity: &str, index: u32, key: &CallKey) {
        let index = i32::try_from(index).unwrap_or(i32::MAX);
        self.keys.set_key(
            &ParticipantIdentity(identity.to_owned()),
            index,
            key.as_bytes().to_vec(),
        );
    }

    /// Sends 48 kHz mono PCM as our microphone, in 10 ms frames, paced by LiveKit's queue.
    pub async fn capture(&self, samples: &[i16]) -> Result<(), Error> {
        for frame in ten_ms_frames(samples) {
            let frame = AudioFrame {
                data: frame.into(),
                sample_rate: SAMPLE_RATE,
                num_channels: 1,
                samples_per_channel: FRAME_SAMPLES as u32,
            };
            self.source
                .capture_frame(&frame)
                .await
                .map_err(|e| Error::LiveKit(e.to_string()))?;
        }
        Ok(())
    }

    /// Sets one of our participant attributes (for example `superdial.state`).
    pub async fn set_attribute(&self, key: &str, value: &str) -> Result<(), Error> {
        let attrs = HashMap::from([(key.to_owned(), value.to_owned())]);
        self.room
            .local_participant()
            .set_attributes(attrs)
            .await
            .map_err(|e| Error::LiveKit(e.to_string()))
    }

    /// Our LiveKit identity, as the server sees it.
    pub fn local_identity(&self) -> &str {
        &self.identity
    }

    /// The LiveKit room name.
    pub fn room_name(&self) -> String {
        self.room.name()
    }

    /// Leaves the LiveKit room.
    pub async fn close(&self) {
        if let Err(e) = self.room.close().await {
            tracing::warn!("closing LiveKit room: {e}");
        }
        self.forwarder.abort();
    }
}

async fn forward_events(mut events: mpsc::UnboundedReceiver<RoomEvent>, tx: mpsc::UnboundedSender<MediaEvent>) {
    let mut readers: Vec<JoinHandle<()>> = Vec::new();
    while let Some(ev) = events.recv().await {
        let out = match ev {
            RoomEvent::TrackSubscribed {
                track: RemoteTrack::Audio(track),
                participant,
                ..
            } => {
                let identity = participant.identity().to_string();
                readers.push(tokio::spawn(read_audio(track, identity, tx.clone())));
                continue;
            }
            RoomEvent::ParticipantConnected(p) => MediaEvent::ParticipantConnected {
                identity: p.identity().to_string(),
            },
            RoomEvent::ParticipantDisconnected(p) => MediaEvent::ParticipantLeft {
                identity: p.identity().to_string(),
            },
            RoomEvent::E2eeStateChanged { participant, state } => MediaEvent::E2ee {
                identity: participant.identity().to_string(),
                state: state.into(),
            },
            RoomEvent::ParticipantAttributesChanged { participant, .. } => MediaEvent::AttributesChanged {
                identity: participant.identity().to_string(),
                attributes: participant.attributes(),
            },
            RoomEvent::Disconnected { reason } => {
                let _ = tx.send(MediaEvent::Disconnected {
                    reason: format!("{reason:?}"),
                });
                break;
            }
            _ => continue,
        };
        if tx.send(out).is_err() {
            break;
        }
    }
    for r in readers {
        r.abort();
    }
}

async fn read_audio(track: RemoteAudioTrack, identity: String, tx: mpsc::UnboundedSender<MediaEvent>) {
    let mut stream = NativeAudioStream::new(track.rtc_track(), SAMPLE_RATE as i32, 1);
    while let Some(frame) = stream.next().await {
        let ev = MediaEvent::Audio {
            identity: identity.clone(),
            samples: frame.data.to_vec(),
        };
        if tx.send(ev).is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use livekit::webrtc::native::frame_cryptor::EncryptionState as Lk;

    #[test]
    fn key_provider_options_match_protocol() {
        let o = key_provider_options();
        assert_eq!(o.ratchet_window_size, 0);
        assert_eq!(o.ratchet_salt, b"LKFrameEncryptionKey".to_vec());
        assert!(matches!(o.key_derivation_algorithm, KeyDerivationAlgorithm::HKDF));
    }

    #[test]
    fn ten_ms_frames_pad_last() {
        let samples: Vec<i16> = (1..=1000).map(|i| i as i16).collect();
        let f = ten_ms_frames(&samples);
        assert_eq!(f.len(), 3);
        assert!(f.iter().all(|x| x.len() == FRAME_SAMPLES));
        assert_eq!(f[0][0], 1);
        assert_eq!(f[2][39], 1000);
        assert!(f[2][40..].iter().all(|s| *s == 0));
    }

    #[test]
    fn ten_ms_frames_empty_input() {
        assert!(ten_ms_frames(&[]).is_empty());
    }

    #[test]
    fn e2ee_state_maps_every_livekit_state() {
        assert_eq!(E2eeState::from(Lk::New), E2eeState::New);
        assert_eq!(E2eeState::from(Lk::Ok), E2eeState::Ok);
        assert_eq!(E2eeState::from(Lk::EncryptionFailed), E2eeState::EncryptionFailed);
        assert_eq!(E2eeState::from(Lk::DecryptionFailed), E2eeState::DecryptionFailed);
        assert_eq!(E2eeState::from(Lk::MissingKey), E2eeState::MissingKey);
        assert_eq!(E2eeState::from(Lk::KeyRatcheted), E2eeState::KeyRatcheted);
        assert_eq!(E2eeState::from(Lk::InternalError), E2eeState::InternalError);
    }

    #[test]
    fn e2ee_state_names() {
        assert_eq!(E2eeState::Ok.as_str(), "Ok");
        assert_eq!(E2eeState::MissingKey.as_str(), "MissingKey");
    }
}
