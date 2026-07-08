//! flashbook-bus: binary event fan-out (Phase 4).
//!
//! [`ring`] is the hand-rolled seqlock broadcast ring (single producer,
//! independent consumers, overwrite-oldest with detected loss). The bench
//! crate races it against `crossbeam-channel` fan-out and
//! `tokio::sync::broadcast` on identical workloads; all three curves are
//! published and the winner ships in the live pipeline.

pub mod ring;

pub use ring::{Consumer, Producer, Recv, Ring, ring, subscribe_ring};
