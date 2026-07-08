//! Exact percentiles from raw samples, nearest-rank method.
//!
//! `P(q) = sorted[ceil(q * n) - 1]` for `q` in (0, 1]. No interpolation:
//! every reported value is a sample that actually occurred. For small `n`
//! the high percentiles saturate at the max — which is honest: with 100
//! samples there IS no p999, and this method reports the max instead of
//! inventing one (`n` is always published alongside).

/// Summary of a raw sample set (units are whatever the samples were in;
/// by convention nanoseconds).
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Percentiles {
    /// Sample count.
    pub n: u64,
    /// Minimum.
    pub min: u64,
    /// Nearest-rank p50.
    pub p50: u64,
    /// Nearest-rank p90.
    pub p90: u64,
    /// Nearest-rank p99.
    pub p99: u64,
    /// Nearest-rank p99.9.
    pub p999: u64,
    /// Maximum.
    pub max: u64,
    /// Arithmetic mean.
    pub mean: f64,
    /// Sample standard deviation (n-1 denominator; 0 for n < 2).
    pub stdev: f64,
}

impl Percentiles {
    /// Compute from unsorted samples. Returns `None` for an empty set —
    /// there is no honest summary of zero samples.
    pub fn from_samples(samples: &[u64]) -> Option<Percentiles> {
        if samples.is_empty() {
            return None;
        }
        let mut s = samples.to_vec();
        s.sort_unstable();
        Some(Self::from_sorted(&s))
    }

    /// Compute from already-sorted samples (ascending).
    pub fn from_sorted(s: &[u64]) -> Percentiles {
        debug_assert!(s.windows(2).all(|w| w[0] <= w[1]), "samples must be sorted");
        let n = s.len();
        let rank = |q: f64| -> u64 {
            #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
            let idx = ((q * n as f64).ceil() as usize).clamp(1, n) - 1;
            s[idx]
        };
        let mean = s.iter().map(|&v| v as f64).sum::<f64>() / n as f64;
        let stdev = if n > 1 {
            let var = s.iter().map(|&v| (v as f64 - mean).powi(2)).sum::<f64>() / (n as f64 - 1.0);
            var.sqrt()
        } else {
            0.0
        };
        Percentiles {
            n: n as u64,
            min: s[0],
            p50: rank(0.50),
            p90: rank(0.90),
            p99: rank(0.99),
            p999: rank(0.999),
            max: s[n - 1],
            mean,
            stdev,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_definition() {
        // 1..=100: p50 = 50th value = 50, p90 = 90, p99 = 99, p999 -> ceil(99.9)=100
        let s: Vec<u64> = (1..=100).collect();
        let p = Percentiles::from_sorted(&s);
        assert_eq!(p.n, 100);
        assert_eq!(p.min, 1);
        assert_eq!(p.p50, 50);
        assert_eq!(p.p90, 90);
        assert_eq!(p.p99, 99);
        assert_eq!(p.p999, 100, "p999 of 100 samples saturates at max");
        assert_eq!(p.max, 100);
        assert!((p.mean - 50.5).abs() < 1e-9);
    }

    #[test]
    fn single_sample_and_unsorted_input() {
        let p = Percentiles::from_samples(&[42]).unwrap();
        assert_eq!((p.min, p.p50, p.p999, p.max, p.n), (42, 42, 42, 42, 1));
        assert_eq!(p.stdev, 0.0);

        let p = Percentiles::from_samples(&[5, 1, 3, 2, 4]).unwrap();
        assert_eq!(p.min, 1);
        assert_eq!(p.p50, 3);
        assert_eq!(p.max, 5);
    }

    #[test]
    fn empty_is_none() {
        assert!(Percentiles::from_samples(&[]).is_none());
    }

    #[test]
    fn large_skewed_distribution() {
        // 999 fast samples at 100ns, one outlier at 1ms: p999 must expose it
        let mut s = vec![100u64; 999];
        s.push(1_000_000);
        s.sort_unstable();
        let p = Percentiles::from_sorted(&s);
        assert_eq!(p.p50, 100);
        assert_eq!(p.p99, 100);
        assert_eq!(p.p999, 100); // ceil(0.999*1000)=999 -> 999th value = 100
        assert_eq!(p.max, 1_000_000); // the outlier lives in max
        let mut s2 = vec![100u64; 998];
        s2.extend([1_000_000, 1_000_000]);
        s2.sort_unstable();
        let p2 = Percentiles::from_sorted(&s2);
        assert_eq!(p2.p999, 1_000_000); // with 2 outliers p999 catches them
    }

    #[test]
    fn serde_roundtrip() {
        let p = Percentiles::from_samples(&[1, 2, 3]).unwrap();
        let j = serde_json::to_string(&p).unwrap();
        let back: Percentiles = serde_json::from_str(&j).unwrap();
        assert_eq!(p, back);
    }
}
