//! Kraken v2 book checksum (CRC32) — the venue-provided correctness oracle
//! for the LOB engine. Stub: implementation lands with the Kraken codec;
//! it lives in proto (not feed) so crates/lob can verify books without a
//! transport dependency (D-006, pending).
