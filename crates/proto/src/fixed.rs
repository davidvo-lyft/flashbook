//! Exact fixed-point decimals at a global scale of 1e-8 (D-003).
//!
//! Venue feeds deliver prices/quantities as ASCII decimal tokens (JSON strings
//! like `"63589.64000000"`, or JSON number tokens like `63589.64`). The hot
//! path parses those bytes directly into an `i64` mantissa at 1e-8 — never
//! through `f64`, so values are exact and book-level identity is well defined.

/// Decimal exponent of the global scale: mantissa 1 == 1e-8 units.
pub const SCALE_EXP: i32 = 8;
/// The global scale as an integer: 1.0 == `SCALE` mantissa units.
pub const SCALE: i64 = 100_000_000;

/// Errors from [`parse_fixed`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ParseFixedError {
    /// Not a decimal token (empty, stray characters, missing digits).
    #[error("malformed decimal token")]
    Malformed,
    /// Magnitude does not fit in an i64 at scale 1e-8.
    #[error("value overflows i64 at scale 1e-8")]
    Overflow,
    /// The token has precision finer than 1e-8; accepting it would silently
    /// round. Per D-003 this is a hard error, not a rounding.
    #[error("precision finer than 1e-8 would be lost")]
    PrecisionLoss,
}

/// Parse an ASCII decimal token into an exact i64 mantissa at scale 1e-8.
///
/// Accepted grammar (no whitespace, no trailing bytes):
/// `[+-]? digits? ('.' digits?)? ([eE] [+-]? digits)?` with at least one
/// digit in the integer+fraction parts. Examples: `"63589.64"`, `"-0.5"`,
/// `".25"`, `"5."`, `"1.2e-3"`, `"0.00000001"`.
#[inline]
pub fn parse_fixed(b: &[u8]) -> Result<i64, ParseFixedError> {
    let n = b.len();
    let mut i = 0usize;
    let neg = match b.first() {
        Some(b'-') => {
            i = 1;
            true
        }
        Some(b'+') => {
            i = 1;
            false
        }
        _ => false,
    };

    let mut mant: u128 = 0;
    let mut any_digit = false;
    while i < n {
        let c = b[i];
        if c.is_ascii_digit() {
            any_digit = true;
            if mant > (u128::MAX - 9) / 10 {
                return Err(ParseFixedError::Overflow);
            }
            mant = mant * 10 + u128::from(c - b'0');
            i += 1;
        } else {
            break;
        }
    }

    let mut frac_digits: i32 = 0;
    if i < n && b[i] == b'.' {
        i += 1;
        while i < n {
            let c = b[i];
            if c.is_ascii_digit() {
                any_digit = true;
                if mant > (u128::MAX - 9) / 10 {
                    return Err(ParseFixedError::Overflow);
                }
                mant = mant * 10 + u128::from(c - b'0');
                frac_digits += 1;
                i += 1;
            } else {
                break;
            }
        }
    }
    if !any_digit {
        return Err(ParseFixedError::Malformed);
    }

    let mut exp: i32 = 0;
    if i < n && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        let eneg = match b.get(i) {
            Some(b'-') => {
                i += 1;
                true
            }
            Some(b'+') => {
                i += 1;
                false
            }
            _ => false,
        };
        let mut any_exp = false;
        let mut e: i32 = 0;
        while i < n {
            let c = b[i];
            if c.is_ascii_digit() {
                any_exp = true;
                // clamp far beyond any representable range; resolved below
                e = e
                    .saturating_mul(10)
                    .saturating_add(i32::from(c - b'0'))
                    .min(10_000);
                i += 1;
            } else {
                break;
            }
        }
        if !any_exp {
            return Err(ParseFixedError::Malformed);
        }
        exp = if eneg { -e } else { e };
    }

    if i != n {
        return Err(ParseFixedError::Malformed);
    }

    scale_mantissa(mant, SCALE_EXP + exp - frac_digits, neg)
}

/// Apply a power of ten `eff` to `mant` and range-check into i64.
fn scale_mantissa(mant: u128, eff: i32, neg: bool) -> Result<i64, ParseFixedError> {
    if mant == 0 {
        return Ok(0);
    }
    let v: u128 = if eff >= 0 {
        if eff > 38 {
            return Err(ParseFixedError::Overflow);
        }
        let pow = 10u128
            .checked_pow(eff.unsigned_abs())
            .ok_or(ParseFixedError::Overflow)?;
        mant.checked_mul(pow).ok_or(ParseFixedError::Overflow)?
    } else {
        let d = eff.unsigned_abs();
        if d > 38 {
            // any nonzero mantissa divided by 10^39+ loses precision
            return Err(ParseFixedError::PrecisionLoss);
        }
        let pow = 10u128.pow(d);
        if !mant.is_multiple_of(pow) {
            return Err(ParseFixedError::PrecisionLoss);
        }
        mant / pow
    };
    let limit = if neg {
        i64::MIN.unsigned_abs() as u128
    } else {
        i64::MAX as u128
    };
    if v > limit {
        return Err(ParseFixedError::Overflow);
    }
    #[allow(clippy::cast_possible_wrap)]
    Ok(if neg {
        (v as i64).wrapping_neg()
    } else {
        v as i64
    })
}

/// Format a 1e-8 mantissa as an exact decimal string (not hot-path).
pub fn format_fixed(m: i64) -> String {
    let a = m.unsigned_abs();
    let int = a / SCALE.unsigned_abs();
    let frac = a % SCALE.unsigned_abs();
    let sign = if m < 0 { "-" } else { "" };
    if frac == 0 {
        format!("{sign}{int}")
    } else {
        let mut f = format!("{frac:08}");
        while f.ends_with('0') {
            f.pop();
        }
        format!("{sign}{int}.{f}")
    }
}

/// Lossy conversion for display/dashboard use only.
#[allow(clippy::cast_precision_loss)]
pub fn fixed_to_f64(m: i64) -> f64 {
    m as f64 / SCALE as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_venue_tokens() {
        // Coinbase/Binance-style strings, Kraken-style number tokens
        assert_eq!(parse_fixed(b"63589.64"), Ok(6_358_964_000_000));
        assert_eq!(parse_fixed(b"63589.64000000"), Ok(6_358_964_000_000));
        assert_eq!(parse_fixed(b"0.00000001"), Ok(1));
        assert_eq!(parse_fixed(b"1"), Ok(SCALE));
        assert_eq!(parse_fixed(b"0"), Ok(0));
        assert_eq!(parse_fixed(b"0.0"), Ok(0));
        assert_eq!(parse_fixed(b"20000"), Ok(2_000_000_000_000));
        assert_eq!(parse_fixed(b"0.12345678"), Ok(12_345_678));
    }

    #[test]
    fn parses_signs_and_edge_shapes() {
        assert_eq!(parse_fixed(b"-1.5"), Ok(-150_000_000));
        assert_eq!(parse_fixed(b"+1.5"), Ok(150_000_000));
        assert_eq!(parse_fixed(b".5"), Ok(50_000_000));
        assert_eq!(parse_fixed(b"5."), Ok(500_000_000));
        assert_eq!(parse_fixed(b"-0"), Ok(0));
        assert_eq!(parse_fixed(b"-0.0000"), Ok(0));
    }

    #[test]
    fn parses_exponent_forms() {
        assert_eq!(parse_fixed(b"1e0"), Ok(SCALE));
        assert_eq!(parse_fixed(b"1e-8"), Ok(1));
        assert_eq!(parse_fixed(b"1.2e-3"), Ok(120_000));
        assert_eq!(parse_fixed(b"1.2E+3"), Ok(120_000_000_000));
        assert_eq!(parse_fixed(b"0e-99999"), Ok(0));
        assert_eq!(parse_fixed(b"0e99999"), Ok(0));
    }

    #[test]
    fn rejects_malformed() {
        for bad in [
            &b""[..],
            b"-",
            b"+",
            b".",
            b"e5",
            b"1e",
            b"1e+",
            b"1..2",
            b"1.2.3",
            b" 1",
            b"1 ",
            b"1,5",
            b"abc",
            b"0x10",
            b"1-",
            b"--1",
        ] {
            assert_eq!(parse_fixed(bad), Err(ParseFixedError::Malformed), "{bad:?}");
        }
    }

    #[test]
    fn rejects_overflow() {
        // i64::MAX at 1e-8 is 92233720368.54775807
        assert_eq!(parse_fixed(b"92233720368.54775807"), Ok(i64::MAX));
        assert_eq!(
            parse_fixed(b"92233720368.54775808"),
            Err(ParseFixedError::Overflow)
        );
        assert_eq!(parse_fixed(b"-92233720368.54775808"), Ok(i64::MIN));
        assert_eq!(
            parse_fixed(b"-92233720368.54775809"),
            Err(ParseFixedError::Overflow)
        );
        assert_eq!(parse_fixed(b"1e30"), Err(ParseFixedError::Overflow));
        assert_eq!(
            parse_fixed(b"99999999999999999999e30"),
            Err(ParseFixedError::Overflow)
        );
    }

    #[test]
    fn rejects_precision_loss() {
        assert_eq!(
            parse_fixed(b"0.000000001"),
            Err(ParseFixedError::PrecisionLoss)
        );
        assert_eq!(
            parse_fixed(b"0.123456789"),
            Err(ParseFixedError::PrecisionLoss)
        );
        assert_eq!(parse_fixed(b"1e-9"), Err(ParseFixedError::PrecisionLoss));
        assert_eq!(parse_fixed(b"1e-40"), Err(ParseFixedError::PrecisionLoss));
        // trailing zeros beyond 8 decimals are exact, not precision loss
        assert_eq!(parse_fixed(b"1.230000000000"), Ok(123_000_000));
    }

    #[test]
    fn format_roundtrips_simple_values() {
        for (m, s) in [
            (0i64, "0"),
            (1, "0.00000001"),
            (SCALE, "1"),
            (-150_000_000, "-1.5"),
            (6_358_964_000_000, "63589.64"),
            (i64::MAX, "92233720368.54775807"),
            (i64::MIN, "-92233720368.54775808"),
        ] {
            assert_eq!(format_fixed(m), s);
            assert_eq!(parse_fixed(s.as_bytes()), Ok(m));
        }
    }
}
