//! flashbook-replay: deterministic replay of captured raw segments.
//!
//! [`source`] discovers per-venue segment files (plain or zstd), reads them
//! with torn-tail salvage, and k-way merges the streams into one
//! deterministic record order keyed by monotonic receive time. The driver
//! layer (codecs -> events -> books, with checksum oracles and digest
//! assertions) composes this with flashbook-feed codecs.

pub mod source;

pub use source::{MergeStats, MergedRecord, MergedStream, SegmentFile, discover, open_segment};
