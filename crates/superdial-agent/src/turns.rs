//! Where one thing a caller said ends: an RMS gate on 10 ms frames, ended by 800 ms of quiet
//! (the spike's detector). Pure: it counts samples, so it runs on synthetic audio in tests.

use std::collections::VecDeque;

/// Samples per millisecond at 48 kHz.
const PER_MS: usize = 48;

/// How utterances are found.
#[derive(Debug, Clone, PartialEq)]
pub struct TurnConfig {
    /// A frame louder than this RMS is speech.
    pub rms_threshold: f64,
    /// This much quiet after speech ends the utterance.
    pub end_silence_ms: u64,
    /// Utterances whose speech (first to last loud frame) is shorter are dropped as noise.
    pub min_voiced_ms: u64,
    /// An utterance is cut here even if the caller is still talking.
    pub max_ms: u64,
    /// Audio kept from just before speech starts, so the first syllable is not clipped.
    pub preroll_ms: u64,
}

impl Default for TurnConfig {
    fn default() -> Self {
        Self {
            rms_threshold: 500.0,
            end_silence_ms: 800,
            min_voiced_ms: 200,
            max_ms: 20_000,
            preroll_ms: 200,
        }
    }
}

/// One thing said: 48 kHz mono PCM and its length.
#[derive(Debug, Clone, PartialEq)]
pub struct Utterance {
    pub samples: Vec<i16>,
    pub audio_ms: u64,
}

/// Finds utterances in one participant's audio.
#[derive(Debug)]
pub struct TurnDetector {
    cfg: TurnConfig,
    /// Recent quiet audio, kept while nobody is speaking.
    preroll: VecDeque<i16>,
    /// The utterance so far, preroll included.
    buf: Vec<i16>,
    in_speech: bool,
    /// Where speech started in `buf`, and where the last loud frame ended.
    voice_start: usize,
    voice_end: usize,
}

impl TurnDetector {
    pub fn new(cfg: TurnConfig) -> Self {
        Self {
            cfg,
            preroll: VecDeque::new(),
            buf: Vec::new(),
            in_speech: false,
            voice_start: 0,
            voice_end: 0,
        }
    }

    fn samples(ms: u64) -> usize {
        ms as usize * PER_MS
    }

    /// Takes the next frame of 48 kHz mono PCM; returns an utterance when this frame ends one.
    pub fn push(&mut self, frame: &[i16]) -> Option<Utterance> {
        let loud = rms(frame) > self.cfg.rms_threshold;
        if !self.in_speech {
            if !loud {
                self.preroll.extend(frame);
                let keep = Self::samples(self.cfg.preroll_ms);
                while self.preroll.len() > keep {
                    self.preroll.pop_front();
                }
                return None;
            }
            self.in_speech = true;
            self.buf = self.preroll.drain(..).collect();
            self.voice_start = self.buf.len();
        }
        self.buf.extend_from_slice(frame);
        if loud {
            self.voice_end = self.buf.len();
        }
        let quiet = self.buf.len() - self.voice_end;
        if quiet < Self::samples(self.cfg.end_silence_ms) && self.buf.len() < Self::samples(self.cfg.max_ms) {
            return None;
        }
        let voiced = self.voice_end - self.voice_start;
        let samples = std::mem::take(&mut self.buf);
        self.reset();
        let audio_ms = (samples.len() / PER_MS) as u64;
        (voiced >= Self::samples(self.cfg.min_voiced_ms)).then_some(Utterance { samples, audio_ms })
    }

    /// Forgets any utterance in progress (used while the agent speaks).
    pub fn reset(&mut self) {
        self.preroll.clear();
        self.buf.clear();
        self.in_speech = false;
        self.voice_start = 0;
        self.voice_end = 0;
    }
}

/// Root mean square of a frame.
pub fn rms(frame: &[i16]) -> f64 {
    if frame.is_empty() {
        return 0.0;
    }
    (frame.iter().map(|s| f64::from(*s).powi(2)).sum::<f64>() / frame.len() as f64).sqrt()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// `ms` of a 440 Hz tone at `amplitude`, 48 kHz.
    pub(crate) fn tone(ms: usize, amplitude: f64) -> Vec<i16> {
        (0..ms * PER_MS)
            .map(|i| (amplitude * (2.0 * std::f64::consts::PI * 440.0 * i as f64 / 48_000.0).sin()) as i16)
            .collect()
    }

    pub(crate) fn silence(ms: usize) -> Vec<i16> {
        vec![0; ms * PER_MS]
    }

    /// Feeds `audio` in 10 ms frames; returns every utterance and the frame index that ended it.
    fn feed(d: &mut TurnDetector, audio: &[i16]) -> Vec<(usize, Utterance)> {
        audio
            .chunks(480)
            .enumerate()
            .filter_map(|(i, f)| d.push(f).map(|u| (i, u)))
            .collect()
    }

    #[test]
    fn one_second_of_speech_then_silence_is_one_utterance() {
        let mut d = TurnDetector::new(TurnConfig::default());
        let mut audio = silence(500);
        audio.extend(tone(1000, 8000.0));
        audio.extend(silence(900));
        let got = feed(&mut d, &audio);
        assert_eq!(got.len(), 1, "{:?}", got.iter().map(|(i, _)| i).collect::<Vec<_>>());
        let (frame, u) = &got[0];
        // 50 frames of leading silence + 100 of tone + the 80th frame of quiet.
        assert_eq!(*frame, 50 + 100 + 79);
        assert!((1800..=2100).contains(&u.audio_ms), "{}", u.audio_ms);
        assert_eq!(u.samples.len() as u64, u.audio_ms * 48);
    }

    #[test]
    fn short_click_is_ignored() {
        let mut d = TurnDetector::new(TurnConfig::default());
        let mut audio = tone(100, 8000.0);
        audio.extend(silence(1500));
        assert!(feed(&mut d, &audio).is_empty());
    }

    #[test]
    fn quiet_noise_is_ignored() {
        let mut d = TurnDetector::new(TurnConfig::default());
        // A sine of amplitude 280 has an RMS of about 198.
        assert!(feed(&mut d, &tone(3000, 280.0)).is_empty());
    }

    #[test]
    fn pause_shorter_than_800ms_does_not_split() {
        let mut d = TurnDetector::new(TurnConfig::default());
        let mut audio = tone(600, 8000.0);
        audio.extend(silence(500));
        audio.extend(tone(600, 8000.0));
        audio.extend(silence(1000));
        let got = feed(&mut d, &audio);
        assert_eq!(got.len(), 1);
        assert!(got[0].1.audio_ms >= 2400, "{}", got[0].1.audio_ms);
    }

    #[test]
    fn two_utterances_apart_are_two() {
        let mut d = TurnDetector::new(TurnConfig::default());
        let mut audio = tone(600, 8000.0);
        audio.extend(silence(1200));
        audio.extend(tone(600, 8000.0));
        audio.extend(silence(1200));
        assert_eq!(feed(&mut d, &audio).len(), 2);
    }

    #[test]
    fn long_speech_is_cut_at_20s() {
        let mut d = TurnDetector::new(TurnConfig::default());
        let got = feed(&mut d, &tone(25_000, 8000.0));
        assert_eq!(got.len(), 1);
        assert!(got[0].1.audio_ms <= 20_000 + 200, "{}", got[0].1.audio_ms);
        assert!(got[0].1.audio_ms >= 19_900);
    }

    #[test]
    fn reset_drops_partial() {
        let mut d = TurnDetector::new(TurnConfig::default());
        assert!(feed(&mut d, &tone(1000, 8000.0)).is_empty());
        d.reset();
        assert!(feed(&mut d, &silence(1500)).is_empty());
    }

    #[test]
    fn rms_of_constant() {
        assert_eq!(rms(&[3, -3, 3, -3]), 3.0);
        assert_eq!(rms(&[]), 0.0);
    }
}
