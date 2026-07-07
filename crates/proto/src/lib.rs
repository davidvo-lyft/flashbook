//! flashbook-proto: the internal binary message format and shared primitives.
//!
//! Everything downstream (feed, lob, store, bus, replay) speaks the types in
//! this crate:
//!
//! - [`fixed`] — exact fixed-point decimal parsing/formatting at a global
//!   scale of 1e-8 (i64 mantissa; no `f64` in the hot path — D-003).
//! - [`event`] — the 64-byte `#[repr(C)]` POD [`event::Event`], read/written
//!   zero-copy via `bytemuck` (D-004).
//! - [`clock`] — monotonic + wall receive timestamps.
//! - [`instrument`] — stable instrument-id registry.
//! - [`rawlog`] — CRC-framed append-only raw capture segments with
//!   torn-write detection (D-001).

#[cfg(not(target_endian = "little"))]
compile_error!("flashbook targets little-endian platforms only (see D-004)");

pub mod clock;
pub mod event;
pub mod fixed;
pub mod instrument;
pub mod kraken_crc;
pub mod rawlog;

pub use event::{Event, EventKind, Side, Venue};
pub use fixed::{ParseFixedError, SCALE, SCALE_EXP, format_fixed, parse_fixed};
pub use instrument::{InstrumentMeta, Registry};
