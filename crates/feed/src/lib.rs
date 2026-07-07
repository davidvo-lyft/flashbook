//! flashbook-feed: per-venue WebSocket feed handlers.
//!
//! Codecs (JSON -> [`flashbook_proto::Event`] normalization) are pure,
//! stateful-but-transport-free objects ([`codec::VenueCodec`]), testable and
//! benchable without a network. The connection layer ([`conn`]) owns
//! sockets, reconnects, and resync; the capture binary composes them with
//! raw-segment sinks ([`sink`]) and periodic stats ([`stats`]).

pub mod binance;
pub mod codec;
pub mod coinbase;
pub mod conn;
pub mod kraken;
pub mod scan;
pub mod sink;
pub mod stats;

pub use codec::{CodecError, CodecStats, Signal, SymbolTable, VenueCodec};
