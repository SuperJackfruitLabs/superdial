//! PCM conversions: 48 kHz (what a call carries) to 16 kHz WAV (what speech-to-text gets), and
//! the speech service's WAV back to 48 kHz mono.

use std::io::Cursor;

/// The call's sample rate.
pub const CALL_RATE: u32 = 48_000;
/// The rate sent to speech-to-text.
pub const STT_RATE: u32 = 16_000;

#[derive(Debug, thiserror::Error)]
pub enum AudioError {
    #[error("not a readable WAV: {0}")]
    Wav(String),
    #[error("unsupported WAV: {0}")]
    Unsupported(String),
}

/// 48 kHz to 16 kHz by averaging each three samples.
pub fn to_16k(samples48: &[i16]) -> Vec<i16> {
    samples48
        .chunks_exact(3)
        .map(|c| ((i32::from(c[0]) + i32::from(c[1]) + i32::from(c[2])) / 3) as i16)
        .collect()
}

/// A 16-bit mono PCM WAV file.
pub fn wav_bytes(samples: &[i16], rate: u32) -> Vec<u8> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: rate,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut out = Cursor::new(Vec::with_capacity(44 + samples.len() * 2));
    {
        let mut w = hound::WavWriter::new(&mut out, spec).expect("WAV header to memory");
        let mut w16 = w.get_i16_writer(samples.len() as u32);
        for s in samples {
            w16.write_sample(*s);
        }
        w16.flush().expect("WAV samples to memory");
        w.finalize().expect("WAV to memory");
    }
    out.into_inner()
}

/// A WAV file (16-bit integer or 32-bit float, any channels and rate) as 48 kHz mono i16.
pub fn wav_to_mono_48k(bytes: &[u8]) -> Result<Vec<i16>, AudioError> {
    let reader = hound::WavReader::new(Cursor::new(bytes)).map_err(|e| AudioError::Wav(e.to_string()))?;
    let spec = reader.spec();
    let channels = usize::from(spec.channels.max(1));
    let interleaved: Vec<f64> = match (spec.sample_format, spec.bits_per_sample) {
        (hound::SampleFormat::Int, 16) => reader
            .into_samples::<i16>()
            .map(|s| s.map(f64::from))
            .collect::<Result<_, _>>(),
        (hound::SampleFormat::Int, bits @ 17..=32) => {
            let scale = f64::from(1u32 << (bits - 16));
            reader
                .into_samples::<i32>()
                .map(|s| s.map(|v| f64::from(v) / scale))
                .collect::<Result<_, _>>()
        }
        (hound::SampleFormat::Float, 32) => reader
            .into_samples::<f32>()
            .map(|s| s.map(|v| f64::from(v) * 32767.0))
            .collect::<Result<_, _>>(),
        (format, bits) => return Err(AudioError::Unsupported(format!("{format:?} {bits}-bit"))),
    }
    .map_err(|e| AudioError::Wav(e.to_string()))?;
    let mono: Vec<f64> = interleaved
        .chunks(channels)
        .map(|c| c.iter().sum::<f64>() / c.len() as f64)
        .collect();
    Ok(resample_linear(&mono, spec.sample_rate, CALL_RATE)
        .into_iter()
        .map(|v| v.round().clamp(f64::from(i16::MIN), f64::from(i16::MAX)) as i16)
        .collect())
}

/// Linear interpolation from `from` Hz to `to` Hz.
fn resample_linear(s: &[f64], from: u32, to: u32) -> Vec<f64> {
    if from == to || s.is_empty() {
        return s.to_vec();
    }
    let out_len = (s.len() as u64 * u64::from(to) / u64::from(from)) as usize;
    let step = f64::from(from) / f64::from(to);
    (0..out_len)
        .map(|j| {
            let pos = j as f64 * step;
            let i = pos.floor() as usize;
            let frac = pos - i as f64;
            let a = s[i.min(s.len() - 1)];
            let b = s[(i + 1).min(s.len() - 1)];
            a + (b - a) * frac
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_16k_thirds_length_and_averages() {
        let s: Vec<i16> = vec![3, 6, 9, -3, -3, -3, 100];
        assert_eq!(to_16k(&s), vec![6, -3]);
        assert_eq!(to_16k(&vec![0; 48_000]).len(), 16_000);
    }

    #[test]
    fn wav_round_trip_16k() {
        let s: Vec<i16> = (0..1600).map(|i| (i * 7 % 2000) as i16 - 1000).collect();
        let bytes = wav_bytes(&s, 16_000);
        assert_eq!(&bytes[..4], b"RIFF");
        let r = hound::WavReader::new(Cursor::new(&bytes)).unwrap();
        assert_eq!(r.spec().sample_rate, 16_000);
        assert_eq!(r.spec().channels, 1);
        let back: Vec<i16> = r.into_samples::<i16>().map(Result::unwrap).collect();
        assert_eq!(back, s);
    }

    #[test]
    fn wav_48k_is_unchanged() {
        let s: Vec<i16> = (0..480).map(|i| i as i16).collect();
        assert_eq!(wav_to_mono_48k(&wav_bytes(&s, 48_000)).unwrap(), s);
    }

    #[test]
    fn wav_24k_becomes_48k_double_length() {
        let s: Vec<i16> = (0..2400).map(|i| (i % 100) as i16 * 10).collect();
        let out = wav_to_mono_48k(&wav_bytes(&s, 24_000)).unwrap();
        assert_eq!(out.len(), 4800);
        assert_eq!(out[0], s[0]);
        assert_eq!(out[2], s[1]);
        // Halfway between neighbours.
        assert_eq!(out[1], (s[0] + s[1]) / 2);
    }

    #[test]
    fn stereo_wav_is_downmixed() {
        let spec = hound::WavSpec {
            channels: 2,
            sample_rate: 48_000,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut buf = Cursor::new(Vec::new());
        {
            let mut w = hound::WavWriter::new(&mut buf, spec).unwrap();
            for _ in 0..10 {
                w.write_sample(1000i16).unwrap();
                w.write_sample(3000i16).unwrap();
            }
            w.finalize().unwrap();
        }
        assert_eq!(wav_to_mono_48k(buf.get_ref()).unwrap(), vec![2000; 10]);
    }

    #[test]
    fn garbage_is_an_error() {
        assert!(wav_to_mono_48k(b"not audio").is_err());
    }
}
