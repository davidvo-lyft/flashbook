//! flashbook-store: append-only columnar tick store (Phase 3).
//!
//! Built from scratch (no storage-engine dependency — that's the point):
//! [`varint`] holds the column encodings (zigzag varint, delta,
//! delta-of-delta), [`block`] the self-delimiting CRC-protected block
//! format with per-block time ranges (the sparse index feeds off these).
//! Higher layers (segment writer/reader, mmap scans, point-in-time snapshot
//! queries) build on these two.

pub mod block;
pub mod varint;
