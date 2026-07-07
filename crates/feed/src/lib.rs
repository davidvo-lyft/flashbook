//! flashbook-feed: per-venue WebSocket feed handlers.
//!
//! Codecs (JSON -> [`flashbook_proto::Event`] normalization) are pure
//! functions, testable and benchable without a network.
