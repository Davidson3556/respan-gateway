//! What a format converts audio with when its spec cannot carry the
//! source's format: G.711 companding (ITU-T G.711) and a streaming
//! windowed-sinc resampler, mono 16-bit, any ratio.

use std::f64::consts::PI;

use respan_gateway_realtime::{AudioFormat, Encoding};

/// The rates a [`Converter`] takes, in samples a second.
pub(crate) const RATES: std::ops::RangeInclusive<u32> = 1_000..=192_000;

/// Audio of one format, chunk by chunk, as another: G.711 decoded,
/// resampled, then encoded as the target says. A chunk may end anywhere,
/// even within a sample: the result is the same however the audio is cut.
#[derive(Debug)]
pub(crate) struct Converter {
    from: Encoding,
    to: Encoding,
    resampler: Option<Resampler>,
    /// A PCM sample's first byte, whose second is the next chunk's.
    odd: Option<u8>,
}

impl Converter {
    /// A converter of `from` to `to`; `None` for an encoding or a rate it
    /// does not take.
    pub(crate) fn new(from: AudioFormat, to: AudioFormat) -> Option<Self> {
        let known = |e| matches!(e, Encoding::Pcm16 | Encoding::Mulaw | Encoding::Alaw);
        if !known(from.encoding) || !known(to.encoding) {
            return None;
        }
        let resampler = match from.rate == to.rate {
            true => None,
            false => Some(Resampler::new(from.rate, to.rate)?),
        };
        Some(Self {
            from: from.encoding,
            to: to.encoding,
            resampler,
            odd: None,
        })
    }

    /// The next `chunk`, converted: what of it is whole so far.
    pub(crate) fn convert(&mut self, chunk: &[u8]) -> Vec<u8> {
        if self.from == self.to && self.resampler.is_none() {
            return chunk.to_vec();
        }
        let mut samples = match self.from {
            Encoding::Mulaw => chunk.iter().map(|&b| mulaw_decode(b)).collect(),
            Encoding::Alaw => chunk.iter().map(|&b| alaw_decode(b)).collect(),
            _ => self.pcm(chunk),
        };
        if let Some(resampler) = &mut self.resampler {
            samples = resampler.push(&samples);
        }
        match self.to {
            Encoding::Mulaw => samples.into_iter().map(mulaw_encode).collect(),
            Encoding::Alaw => samples.into_iter().map(alaw_encode).collect(),
            _ => samples.into_iter().flat_map(i16::to_le_bytes).collect(),
        }
    }

    /// The whole samples of `chunk` after any byte held from the last.
    fn pcm(&mut self, chunk: &[u8]) -> Vec<i16> {
        let mut bytes = chunk;
        let mut samples = Vec::with_capacity(chunk.len() / 2 + 1);
        if let (Some(first), [second, rest @ ..]) = (self.odd, bytes) {
            samples.push(i16::from_le_bytes([first, *second]));
            self.odd = None;
            bytes = rest;
        }
        let pairs = bytes.chunks_exact(2);
        if let [last] = pairs.remainder() {
            self.odd = Some(*last);
        }
        samples.extend(pairs.map(|pair| i16::from_le_bytes([pair[0], pair[1]])));
        samples
    }
}

/// G.711 μ-law's bias, added before a sample is companded.
const MULAW_BIAS: i32 = 0x84;

/// A G.711 μ-law byte's sample.
pub(crate) fn mulaw_decode(byte: u8) -> i16 {
    let byte = !byte;
    let magnitude = ((i32::from(byte & 0x0F) << 3) + MULAW_BIAS) << ((byte & 0x70) >> 4);
    let sample = match byte & 0x80 {
        0 => magnitude - MULAW_BIAS,
        _ => MULAW_BIAS - magnitude,
    };
    sample as i16
}

/// A sample as a G.711 μ-law byte.
pub(crate) fn mulaw_encode(sample: i16) -> u8 {
    let sign = if sample < 0 { 0x80 } else { 0 };
    let magnitude = i32::from(sample).abs().min(32_635) + MULAW_BIAS;
    let exponent = (31 - (magnitude >> 7).leading_zeros()).min(7) as u8;
    let mantissa = ((magnitude >> (exponent + 3)) & 0x0F) as u8;
    !(sign | (exponent << 4) | mantissa)
}

/// A G.711 A-law byte's sample.
pub(crate) fn alaw_decode(byte: u8) -> i16 {
    let byte = byte ^ 0x55;
    let mantissa = i32::from(byte & 0x0F) << 4;
    let segment = (byte & 0x70) >> 4;
    let magnitude = match segment {
        0 => mantissa + 8,
        _ => (mantissa + 0x108) << (segment - 1),
    };
    match byte & 0x80 {
        0 => -magnitude as i16,
        _ => magnitude as i16,
    }
}

/// A sample as a G.711 A-law byte.
pub(crate) fn alaw_encode(sample: i16) -> u8 {
    let (mask, magnitude) = match sample >= 0 {
        true => (0xD5, i32::from(sample) >> 3),
        false => (0x55, -(i32::from(sample) >> 3) - 1),
    };
    let segment = (32 - (magnitude >> 5).leading_zeros()) as u8;
    let byte = match segment {
        8.. => 0x7F,
        0 => (magnitude >> 1) as u8 & 0x0F,
        _ => (segment << 4) | ((magnitude >> segment) as u8 & 0x0F),
    };
    byte ^ mask
}

/// Zero crossings of the filter's sinc on each side, at its cutoff.
const ZERO_CROSSINGS: f64 = 16.0;
/// The cutoff, as a share of the lower rate's Nyquist frequency.
const ROLLOFF: f64 = 0.94;
/// The Kaiser window's shape: sidelobes about 86 dB down.
const BETA: f64 = 8.6;
/// Most coefficients a resampler keeps, one set per phase; past it each
/// output sample computes its own.
const TABLE: usize = 1 << 16;

/// A streaming windowed-sinc resampler: mono 16-bit samples at one rate
/// in, at another out. It keeps the samples its filter still needs and
/// where the next output falls between them, so the result is the same
/// however the input is cut. An output waits for the input after it that
/// its filter reads, a few milliseconds of it.
#[derive(Debug)]
pub(crate) struct Resampler {
    /// Output samples per `down` input samples, in lowest terms.
    up: u64,
    /// Input samples per `up` output samples.
    down: u64,
    /// The cutoff, as a share of the input's Nyquist frequency.
    cutoff: f64,
    /// Taps on each side of an output sample.
    half: usize,
    /// Each phase's taps, if they fit [`TABLE`].
    table: Option<Vec<f32>>,
    /// The input samples still needed, the first at `start`.
    history: Vec<f32>,
    /// The input index of `history[0]`; before the input are zeros.
    start: i64,
    /// The index of the next output sample.
    next: u64,
}

impl Resampler {
    /// A resampler of `from` samples a second to `to`; `None` for a rate
    /// outside [`RATES`].
    pub(crate) fn new(from: u32, to: u32) -> Option<Self> {
        if !RATES.contains(&from) || !RATES.contains(&to) {
            return None;
        }
        let gcd = gcd(u64::from(from), u64::from(to));
        let (up, down) = (u64::from(to) / gcd, u64::from(from) / gcd);
        let cutoff = ROLLOFF * (f64::from(to) / f64::from(from)).min(1.0);
        let half = (ZERO_CROSSINGS / cutoff).ceil() as usize;
        let mut resampler = Self {
            up,
            down,
            cutoff,
            half,
            table: None,
            history: vec![0.0; half],
            start: -(half as i64),
            next: 0,
        };
        if (up as usize).saturating_mul(2 * half) <= TABLE {
            let mut table = vec![0.0; up as usize * 2 * half];
            for (phase, taps) in table.chunks_exact_mut(2 * half).enumerate() {
                resampler.taps(phase as u64, taps);
            }
            resampler.table = Some(table);
        }
        Some(resampler)
    }

    /// The output samples `input` completes.
    pub(crate) fn push(&mut self, input: &[i16]) -> Vec<i16> {
        self.history.extend(input.iter().map(|&s| f32::from(s)));
        let end = self.start + self.history.len() as i64;
        let mut out = Vec::new();
        let mut scratch = vec![0.0; 2 * self.half];
        loop {
            let at = self.next * self.down;
            let (index, phase) = ((at / self.up) as i64, at % self.up);
            // The taps read `index - half + 1 ..= index + half`.
            if index + self.half as i64 >= end {
                break;
            }
            let taps = match &self.table {
                Some(table) => {
                    let width = 2 * self.half;
                    &table[phase as usize * width..][..width]
                }
                None => {
                    self.taps(phase, &mut scratch);
                    &scratch
                }
            };
            let first = (index - self.start) as usize + 1 - self.half;
            let window = &self.history[first..first + taps.len()];
            let sum: f32 = window.iter().zip(taps).map(|(x, h)| x * h).sum();
            out.push(sum.round().clamp(-32_768.0, 32_767.0) as i16);
            self.next += 1;
        }
        // Keep what the next output reads.
        let first =
            ((self.next * self.down / self.up) as i64 + 1 - self.half as i64).max(self.start);
        self.history.drain(..(first - self.start) as usize);
        self.start = first;
        out
    }

    /// The taps of `phase`, an output `phase / up` of the way past an input
    /// sample, normalized to unit gain.
    fn taps(&self, phase: u64, taps: &mut [f32]) {
        let fraction = phase as f64 / self.up as f64;
        let half = self.half as f64;
        let h = |i: usize| {
            // Input `index - half + 1 + i`, `t` input samples from the output.
            let t = i as f64 + 1.0 - half - fraction;
            let window = (1.0 - (t / half).powi(2)).max(0.0);
            self.cutoff * sinc(self.cutoff * t) * bessel_i0(BETA * window.sqrt())
        };
        let values: Vec<f64> = (0..taps.len()).map(h).collect();
        let sum: f64 = values.iter().sum();
        for (tap, h) in taps.iter_mut().zip(values) {
            *tap = (h / sum) as f32;
        }
    }
}

/// `sin(πx) / πx`.
fn sinc(x: f64) -> f64 {
    match x.abs() < 1e-12 {
        true => 1.0,
        false => (PI * x).sin() / (PI * x),
    }
}

/// The modified Bessel function of the first kind, order 0, by its series.
fn bessel_i0(x: f64) -> f64 {
    let (mut sum, mut term, mut k) = (1.0, 1.0, 1.0);
    while term > sum * 1e-12 {
        term *= (x / (2.0 * k)).powi(2);
        sum += term;
        k += 1.0;
    }
    sum
}

fn gcd(a: u64, b: u64) -> u64 {
    match b {
        0 => a,
        _ => gcd(b, a % b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mulaw_meets_g711s_reference_points() {
        // Codes and the 16-bit values of ITU-T G.711's μ-law decoder.
        for (code, sample) in [
            (0xFF, 0),
            (0x7F, 0),
            (0xFE, 8),
            (0xF0, 120),
            (0xEF, 132),
            (0x80, 32_124),
            (0x00, -32_124),
            (0x70, -120),
        ] {
            assert_eq!(mulaw_decode(code), sample, "{code:#04x}");
        }
        assert_eq!(mulaw_encode(0), 0xFF);
        assert_eq!(mulaw_encode(i16::MAX), 0x80);
        assert_eq!(mulaw_encode(i16::MIN), 0x00);
        // Every code is its sample's, but negative zero, which is zero's.
        for code in 0..=255u8 {
            let again = mulaw_encode(mulaw_decode(code));
            assert_eq!(again, if code == 0x7F { 0xFF } else { code });
        }
    }

    #[test]
    fn alaw_meets_g711s_reference_points() {
        for (code, sample) in [
            (0xD5, 8),
            (0x55, -8),
            (0xD4, 24),
            (0xC5, 264),
            (0xAA, 32_256),
            (0x2A, -32_256),
        ] {
            assert_eq!(alaw_decode(code), sample, "{code:#04x}");
        }
        assert_eq!(alaw_encode(0), 0xD5);
        assert_eq!(alaw_encode(-1), 0x55);
        assert_eq!(alaw_encode(i16::MAX), 0xAA);
        assert_eq!(alaw_encode(i16::MIN), 0x2A);
        for code in 0..=255u8 {
            assert_eq!(alaw_encode(alaw_decode(code)), code, "{code:#04x}");
        }
    }

    #[test]
    fn companding_keeps_a_samples_size() {
        for sample in (-32_768..=32_767).step_by(97) {
            let sample = sample as i16;
            let error = |back: i16| (f64::from(back) - f64::from(sample)).abs();
            // A step is at most 1/16 of its segment's top.
            let bound = f64::from(sample).abs() / 16.0 + 16.0;
            assert!(
                error(mulaw_decode(mulaw_encode(sample))) <= bound,
                "{sample}"
            );
            assert!(error(alaw_decode(alaw_encode(sample))) <= bound, "{sample}");
        }
    }

    /// `seconds` of a sine of `hz` at `rate`, at half of full scale.
    fn sine(hz: f64, rate: u32, seconds: f64) -> Vec<i16> {
        let n = (f64::from(rate) * seconds) as usize;
        let at = |i: usize| (2.0 * PI * hz * i as f64 / f64::from(rate)).sin();
        (0..n).map(|i| (16_384.0 * at(i)).round() as i16).collect()
    }

    /// The power of `samples`, edges skipped, where the filter starts from
    /// silence.
    fn power(samples: impl Iterator<Item = f64>) -> f64 {
        let samples: Vec<f64> = samples.collect();
        let middle = &samples[200..samples.len() - 200];
        middle.iter().map(|s| s * s).sum::<f64>() / middle.len() as f64
    }

    #[test]
    fn resampling_up_and_down_keeps_a_tone() {
        for from in [16_000, 48_000, 8_000, 44_100] {
            let out = Resampler::new(from, 24_000)
                .expect("rates")
                .push(&sine(1_000.0, from, 0.5));
            let ideal = sine(1_000.0, 24_000, 0.5);
            assert!(out.len() > ideal.len() - 100, "{from}: {}", out.len());
            let signal = power(ideal.iter().take(out.len()).map(|&s| f64::from(s)));
            let noise = power(
                out.iter()
                    .zip(&ideal)
                    .map(|(&o, &i)| f64::from(o) - f64::from(i)),
            );
            let snr = 10.0 * (signal / noise).log10();
            assert!(snr > 60.0, "{from} to 24000: {snr:.1} dB");
        }
    }

    #[test]
    fn resampling_down_removes_what_the_rate_cannot_carry() {
        let input = sine(5_000.0, 24_000, 0.5);
        let out = Resampler::new(24_000, 8_000).expect("rates").push(&input);
        assert!(out.len() > 3_900);
        let before = power(input.iter().map(|&s| f64::from(s)));
        let after = power(out.iter().map(|&s| f64::from(s)));
        let attenuation = 10.0 * (before / after.max(1e-9)).log10();
        assert!(attenuation > 50.0, "{attenuation:.1} dB");
    }

    /// A pseudo-random sequence, the same each run.
    fn lcg(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        *seed >> 33
    }

    #[test]
    fn a_converters_result_is_the_same_however_its_audio_is_cut() {
        let pcm: Vec<u8> = sine(440.0, 16_000, 0.3)
            .into_iter()
            .flat_map(i16::to_le_bytes)
            .collect();
        let mulaw: Vec<u8> = pcm
            .chunks_exact(2)
            .map(|p| mulaw_encode(i16::from_le_bytes([p[0], p[1]])))
            .collect();
        for (from, to, audio) in [
            (AudioFormat::pcm16(16_000), AudioFormat::pcm16(24_000), &pcm),
            (AudioFormat::pcm16(16_000), AudioFormat::MULAW, &pcm),
            (AudioFormat::MULAW, AudioFormat::pcm16(24_000), &mulaw),
            (AudioFormat::MULAW, AudioFormat::ALAW, &mulaw),
            (AudioFormat::pcm16(16_000), AudioFormat::pcm16(16_000), &pcm),
        ] {
            let whole = Converter::new(from, to).expect("formats").convert(audio);
            let mut seed = 7;
            for _ in 0..5 {
                let mut converter = Converter::new(from, to).expect("formats");
                let mut cut = Vec::new();
                let mut rest = &audio[..];
                while !rest.is_empty() {
                    // Odd lengths split samples between chunks.
                    let n = (lcg(&mut seed) as usize % 701).min(rest.len());
                    cut.extend(converter.convert(&rest[..n]));
                    rest = &rest[n..];
                }
                assert_eq!(cut, whole, "{from:?} to {to:?}");
            }
        }
    }

    #[test]
    fn a_pcm_converter_holds_a_split_sample_until_it_is_whole() {
        let mut converter =
            Converter::new(AudioFormat::pcm16(24_000), AudioFormat::MULAW).expect("formats");
        assert!(converter.convert(&[0x34]).is_empty());
        assert_eq!(converter.odd, Some(0x34));
        let mut whole =
            Converter::new(AudioFormat::pcm16(24_000), AudioFormat::MULAW).expect("formats");
        let mut cut = converter.convert(&[0x12, 0x00]);
        cut.extend(converter.convert(&[0x7F]));
        assert_eq!(cut, whole.convert(&[0x34, 0x12, 0x00, 0x7F]));
    }

    #[test]
    fn rates_out_of_range_are_refused() {
        assert!(Resampler::new(0, 24_000).is_none());
        assert!(Resampler::new(24_000, 1_000_000).is_none());
        assert!(Converter::new(AudioFormat::pcm16(16_000), AudioFormat::pcm16(0)).is_none());
        // A rate of no common ratio computes its taps per sample.
        let mut odd = Resampler::new(44_101, 24_000).expect("rates");
        assert!(odd.table.is_none());
        assert!(odd.push(&sine(1_000.0, 44_101, 0.01)).len() > 200);
    }
}
