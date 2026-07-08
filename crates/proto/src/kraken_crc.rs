//! Kraken v2 book checksum (CRC32) — the venue-provided correctness oracle
//! for the LOB engine. It lives in proto (not feed) so crates/lob can verify
//! books without a transport dependency (D-006).
//!
//! Kraken's documented algorithm (spot WS v2 book channel): take the top 10
//! ask levels (ascending by price), then the top 10 bid levels (descending
//! by price). For each level, format the price to the pair's price-precision
//! decimal places, remove the decimal point, strip leading zeros, and append
//! the digits; then do the same for the quantity at the pair's
//! quantity-precision. The checksum is CRC32 (IEEE, as in `crc32fast`) over
//! the concatenated ASCII digits.
//!
//! Verified empirically against 2962 consecutive live book messages
//! (crates/feed/fixtures/kraken/live-btc-eth.ndjson): every venue-sent
//! checksum matches when the local book is truncated to the subscription
//! depth after each update.

/// Append the ASCII digits of a 1e-8 mantissa formatted at `dec` decimal
/// places with the decimal point removed and leading zeros stripped.
///
/// Stripping every leading zero means a zero value contributes no bytes at
/// all — exactly what Kraken's algorithm produces for `0.00000000`.
///
/// A mantissa that is not representable at `dec` decimals (not divisible by
/// `10^(8-dec)`) is a contract violation: the caller fed a price/qty finer
/// than the pair's precision. We `debug_assert` on it and, in release,
/// saturate via integer division (truncation toward zero), which keeps the
/// function total but will produce a checksum mismatch downstream — the
/// desired failure mode for corrupt input.
fn update_scaled(hasher: &mut crc32fast::Hasher, m: i64, dec: u32) {
    debug_assert!(dec <= 8, "decimals beyond the global 1e-8 scale");
    debug_assert!(m >= 0, "book prices/quantities are non-negative");
    let div = 10i64.pow(8 - dec.min(8));
    debug_assert!(
        m % div == 0,
        "mantissa {m} not representable at {dec} decimals"
    );
    let mut v = (m / div).max(0).unsigned_abs();
    if v == 0 {
        return;
    }
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    while v > 0 {
        i -= 1;
        buf[i] = b'0' + (v % 10) as u8;
        v /= 10;
    }
    hasher.update(&buf[i..]);
}

/// Compute Kraken's v2 book checksum over the top-10 levels of each side.
///
/// `asks` must be the best (lowest-price-first) ask levels and `bids` the
/// best (highest-price-first) bid levels; entries beyond the first 10 per
/// side are ignored, matching the venue (the checksum always covers only the
/// top 10 regardless of subscription depth). `(price, qty)` are exact i64
/// mantissas at the global 1e-8 scale. `price_dec` / `qty_dec` are the
/// pair's venue precisions (see `flashbook_feed::kraken::pair_decimals`).
///
/// Allocation-free: digits are formatted into a stack buffer and streamed
/// into the CRC hasher.
#[must_use]
pub fn kraken_book_crc32(
    asks: &[(i64, i64)],
    bids: &[(i64, i64)],
    price_dec: u32,
    qty_dec: u32,
) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    for &(price, qty) in asks.iter().take(10) {
        update_scaled(&mut hasher, price, price_dec);
        update_scaled(&mut hasher, qty, qty_dec);
    }
    for &(price, qty) in bids.iter().take(10) {
        update_scaled(&mut hasher, price, price_dec);
        update_scaled(&mut hasher, qty, qty_dec);
    }
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-verified against the first BTC/USD snapshot of the live capture
    /// (checksum 423379305, BTC/USD price_dec=1, qty_dec=8).
    #[test]
    fn golden_btc_snapshot() {
        let asks: [(i64, i64); 10] = [
            (6_350_190_000_000, 100_000),
            (6_350_200_000_000, 73_311),
            (6_350_210_000_000, 1_946_065),
            (6_350_220_000_000, 6_514_381),
            (6_350_400_000_000, 5_100),
            (6_350_470_000_000, 47_474_367),
            (6_350_480_000_000, 78_734_230),
            (6_350_510_000_000, 786_998),
            (6_350_590_000_000, 78_732_894),
            (6_350_640_000_000, 783_883),
        ];
        let bids: [(i64, i64); 10] = [
            (6_350_160_000_000, 63_889_204),
            (6_350_090_000_000, 5_100),
            (6_349_900_000_000, 78_741_286),
            (6_349_800_000_000, 130_271_205),
            (6_349_770_000_000, 5_100),
            (6_349_700_000_000, 15_000),
            (6_349_640_000_000, 27_044_082),
            (6_349_630_000_000, 81_891_828),
            (6_349_580_000_000, 2_713_013),
            (6_349_520_000_000, 5_990_000),
        ];
        assert_eq!(kraken_book_crc32(&asks, &bids, 1, 8), 423_379_305);
    }

    /// 5- and 7-decimal price formatting (XRP/DOGE style), cross-checked
    /// against a Python zlib.crc32 reference implementation.
    #[test]
    fn golden_high_decimal_pairs() {
        // DOGE-style: price_dec=7. price 0.1234567, qty 2.5 / bid 0.1234560, 1e-6.
        assert_eq!(
            kraken_book_crc32(&[(12_345_670, 250_000_000)], &[(12_345_600, 100)], 7, 8),
            1_347_474_524
        );
        // XRP-style: price_dec=5. price 0.52345, qty 1000 / bid 0.52344, 1e-8.
        assert_eq!(
            kraken_book_crc32(&[(52_345_000, 100_000_000_000)], &[(52_344_000, 1)], 5, 8),
            984_447_197
        );
    }

    /// Only the top 10 levels per side count, even if more are passed.
    #[test]
    fn truncates_to_top_ten() {
        let asks: Vec<(i64, i64)> = (1..=12).map(|i| (100_000_000 * i, 100_000_000)).collect();
        let bids: Vec<(i64, i64)> = (0..12)
            .map(|i| (100_000_000 * (50 - i), 200_000_000))
            .collect();
        let full = kraken_book_crc32(&asks, &bids, 1, 8);
        let top10 = kraken_book_crc32(&asks[..10], &bids[..10], 1, 8);
        assert_eq!(full, top10);
        assert_eq!(full, 1_138_457_654); // python zlib.crc32 reference
    }

    /// Zero quantities contribute no bytes; the empty book hashes to 0.
    #[test]
    fn zero_values_and_empty_book() {
        assert_eq!(kraken_book_crc32(&[], &[], 1, 8), 0);
        // a level with qty 0 contributes only its price digits
        let with_zero_qty = kraken_book_crc32(&[(100_000_000, 0)], &[], 1, 8);
        let price_only = crc32fast::hash(b"10");
        assert_eq!(with_zero_qty, price_only);
    }
}
