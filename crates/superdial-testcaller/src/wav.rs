//! WAV in and out at 48 kHz mono, and the measurements the integration test asserts on.

use std::path::Path;

use anyhow::{Context, Result};

const RATE: u32 = 48_000;

/// Reads a WAV file as 48 kHz mono i16: channels averaged, linear resampling.
pub fn read_mono_48k(path: &Path) -> Result<Vec<i16>> {
    let mut r = hound::WavReader::open(path).with_context(|| format!("open {}", path.display()))?;
    let spec = r.spec();
    let interleaved: Vec<f64> = match spec.sample_format {
        hound::SampleFormat::Int => {
            let shift = spec.bits_per_sample.saturating_sub(16);
            r.samples::<i32>()
                .map(|s| s.map(|v| f64::from(v >> shift)))
                .collect::<Result<_, _>>()?
        }
        hound::SampleFormat::Float => r
            .samples::<f32>()
            .map(|s| s.map(|v| f64::from(v) * 32767.0))
            .collect::<Result<_, _>>()?,
    };
    let ch = usize::from(spec.channels.max(1));
    let mono: Vec<f64> = interleaved
        .chunks(ch)
        .map(|f| f.iter().sum::<f64>() / f.len() as f64)
        .collect();
    Ok(resample(&mono, spec.sample_rate, RATE))
}

fn resample(x: &[f64], from: u32, to: u32) -> Vec<i16> {
    let clamp = |v: f64| v.round().clamp(-32768.0, 32767.0) as i16;
    if from == to || x.is_empty() {
        return x.iter().map(|v| clamp(*v)).collect();
    }
    let n = (x.len() as u64 * u64::from(to) / u64::from(from)) as usize;
    (0..n)
        .map(|i| {
            let pos = i as f64 * f64::from(from) / f64::from(to);
            let j = pos.floor() as usize;
            let frac = pos - j as f64;
            let a = x[j.min(x.len() - 1)];
            let b = x[(j + 1).min(x.len() - 1)];
            clamp(a + (b - a) * frac)
        })
        .collect()
}

/// Writes 48 kHz mono i16 PCM.
pub fn write_mono_48k(path: &Path, samples: &[i16]) -> Result<()> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: RATE,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut w = hound::WavWriter::create(path, spec).with_context(|| format!("create {}", path.display()))?;
    for s in samples {
        w.write_sample(*s)?;
    }
    w.finalize()?;
    Ok(())
}

/// Root mean square of the samples.
pub fn rms(samples: &[i16]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    (samples.iter().map(|s| f64::from(*s).powi(2)).sum::<f64>() / samples.len() as f64).sqrt()
}

/// The share of the signal's power at `hz` (0 to about 1), from a Goertzel filter over 100 ms
/// windows, weighted by each window's power.
pub fn tone_ratio(samples: &[i16], hz: f64) -> f64 {
    let (mut tone, mut total) = (0.0, 0.0);
    for w in samples.chunks(4800) {
        let n = w.len() as f64;
        let coeff = 2.0 * (2.0 * std::f64::consts::PI * hz / f64::from(RATE)).cos();
        let (mut s1, mut s2) = (0.0f64, 0.0f64);
        for x in w {
            let s0 = f64::from(*x) + coeff * s1 - s2;
            s2 = s1;
            s1 = s0;
        }
        let power = s1 * s1 + s2 * s2 - coeff * s1 * s2;
        tone += 2.0 * power / n;
        total += w.iter().map(|x| f64::from(*x).powi(2)).sum::<f64>();
    }
    if total == 0.0 {
        0.0
    } else {
        tone / total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("superdial-wav-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    fn sine(hz: f64, rate: u32, n: usize) -> Vec<i16> {
        (0..n)
            .map(|i| (8000.0 * (2.0 * std::f64::consts::PI * hz * i as f64 / rate as f64).sin()) as i16)
            .collect()
    }

    fn write(path: &std::path::Path, rate: u32, channels: u16, samples: &[i16]) {
        let spec = hound::WavSpec {
            channels,
            sample_rate: rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut w = hound::WavWriter::create(path, spec).unwrap();
        for s in samples {
            w.write_sample(*s).unwrap();
        }
        w.finalize().unwrap();
    }

    #[test]
    fn wav_round_trip() {
        let p = tmp("rt.wav");
        let s = sine(440.0, 48_000, 4800);
        write_mono_48k(&p, &s).unwrap();
        assert_eq!(read_mono_48k(&p).unwrap(), s);
    }

    #[test]
    fn resample_24k_doubles_length() {
        let p = tmp("24k.wav");
        write(&p, 24_000, 1, &sine(440.0, 24_000, 2400));
        assert_eq!(read_mono_48k(&p).unwrap().len(), 4800);
    }

    #[test]
    fn stereo_is_downmixed() {
        let p = tmp("st.wav");
        // Left 1000, right 3000, interleaved.
        let frames: Vec<i16> = (0..100).flat_map(|_| [1000i16, 3000]).collect();
        write(&p, 48_000, 2, &frames);
        let m = read_mono_48k(&p).unwrap();
        assert_eq!(m.len(), 100);
        assert!(m.iter().all(|s| *s == 2000));
    }

    #[test]
    fn rms_of_constant() {
        assert!((rms(&[3, -3, 3, -3]) - 3.0).abs() < 1e-9);
        assert_eq!(rms(&[]), 0.0);
    }

    #[test]
    fn tone_ratio_high_for_matching_tone_low_for_other() {
        let s = sine(440.0, 48_000, 48_000);
        assert!(tone_ratio(&s, 440.0) > 0.9, "{}", tone_ratio(&s, 440.0));
        assert!(tone_ratio(&s, 660.0) < 0.05, "{}", tone_ratio(&s, 660.0));
        assert_eq!(tone_ratio(&[0; 4800], 440.0), 0.0);
    }
}
