//! Stable instrument-id registry. Ids are fixed and committed (see
//! [`Registry::builtin`]); replay determinism and the tick store depend on
//! ids never changing meaning.

use std::collections::HashMap;

use crate::event::Venue;

/// One instrument on one venue.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InstrumentMeta {
    /// Stable id (never reused, never renumbered).
    pub id: u32,
    /// Venue.
    pub venue: Venue,
    /// Venue-native symbol as used by its REST API (e.g. `BTC-USD`,
    /// `BTCUSDT`, `BTC/USD`).
    pub venue_symbol: String,
    /// Cross-venue canonical name, e.g. `BTC-USD`.
    pub canonical: String,
}

/// Lookup table over [`InstrumentMeta`].
#[derive(Debug, Clone, Default)]
pub struct Registry {
    metas: Vec<InstrumentMeta>,
    by_id: HashMap<u32, usize>,
    by_venue_symbol: HashMap<(Venue, String), u32>,
}

/// Registry construction errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RegistryError {
    /// Two entries share an id.
    #[error("duplicate instrument id {0}")]
    DuplicateId(u32),
    /// Two entries share (venue, venue_symbol).
    #[error("duplicate venue symbol {0}")]
    DuplicateSymbol(String),
    /// Id 0 is reserved as "unknown".
    #[error("instrument id 0 is reserved")]
    ReservedId,
}

impl Registry {
    /// Build a registry from metas, validating uniqueness.
    pub fn new(metas: Vec<InstrumentMeta>) -> Result<Self, RegistryError> {
        let mut r = Registry::default();
        for m in metas {
            r.insert(m)?;
        }
        Ok(r)
    }

    /// Insert one meta, validating uniqueness.
    pub fn insert(&mut self, m: InstrumentMeta) -> Result<(), RegistryError> {
        if m.id == 0 {
            return Err(RegistryError::ReservedId);
        }
        if self.by_id.contains_key(&m.id) {
            return Err(RegistryError::DuplicateId(m.id));
        }
        let key = (m.venue, m.venue_symbol.clone());
        if self.by_venue_symbol.contains_key(&key) {
            return Err(RegistryError::DuplicateSymbol(m.venue_symbol));
        }
        self.by_id.insert(m.id, self.metas.len());
        self.by_venue_symbol.insert(key, m.id);
        self.metas.push(m);
        Ok(())
    }

    /// Meta by id.
    pub fn get(&self, id: u32) -> Option<&InstrumentMeta> {
        self.by_id.get(&id).map(|&i| &self.metas[i])
    }

    /// Id by (venue, venue-native symbol).
    pub fn id_of(&self, venue: Venue, venue_symbol: &str) -> Option<u32> {
        self.by_venue_symbol
            .get(&(venue, venue_symbol.to_string()))
            .copied()
    }

    /// All metas.
    pub fn all(&self) -> &[InstrumentMeta] {
        &self.metas
    }

    /// Metas for one venue.
    pub fn for_venue(&self, venue: Venue) -> impl Iterator<Item = &InstrumentMeta> {
        self.metas.iter().filter(move |m| m.venue == venue)
    }

    /// Serialize to JSON (sidecar file format).
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(&self.metas).expect("registry serializes")
    }

    /// Deserialize from JSON produced by [`Registry::to_json`].
    pub fn from_json(s: &str) -> Result<Self, String> {
        let metas: Vec<InstrumentMeta> = serde_json::from_str(s).map_err(|e| e.to_string())?;
        Registry::new(metas).map_err(|e| e.to_string())
    }

    /// The committed builtin universe: BTC, ETH, SOL, XRP, DOGE vs USD
    /// (USDT on Binance). Ids: Coinbase 1-5, Binance 6-10, Kraken 11-15.
    pub fn builtin() -> Self {
        const SYMS: [&str; 5] = ["BTC", "ETH", "SOL", "XRP", "DOGE"];
        let mut metas = Vec::new();
        for (vi, venue) in Venue::ALL.iter().enumerate() {
            for (si, base) in SYMS.iter().enumerate() {
                #[allow(clippy::cast_possible_truncation)]
                let id = (vi * 5 + si + 1) as u32;
                let venue_symbol = match venue {
                    Venue::Coinbase => format!("{base}-USD"),
                    Venue::Binance => format!("{base}USDT"),
                    Venue::Kraken => format!("{base}/USD"),
                };
                metas.push(InstrumentMeta {
                    id,
                    venue: *venue,
                    venue_symbol,
                    canonical: format!("{base}-USD"),
                });
            }
        }
        Registry::new(metas).expect("builtin registry is valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_ids_are_stable() {
        let r = Registry::builtin();
        assert_eq!(r.all().len(), 15);
        assert_eq!(r.id_of(Venue::Coinbase, "BTC-USD"), Some(1));
        assert_eq!(r.id_of(Venue::Coinbase, "DOGE-USD"), Some(5));
        assert_eq!(r.id_of(Venue::Binance, "BTCUSDT"), Some(6));
        assert_eq!(r.id_of(Venue::Binance, "DOGEUSDT"), Some(10));
        assert_eq!(r.id_of(Venue::Kraken, "BTC/USD"), Some(11));
        assert_eq!(r.id_of(Venue::Kraken, "DOGE/USD"), Some(15));
        assert_eq!(r.get(11).unwrap().canonical, "BTC-USD");
    }

    #[test]
    fn json_roundtrip() {
        let r = Registry::builtin();
        let j = r.to_json();
        let r2 = Registry::from_json(&j).unwrap();
        assert_eq!(r.all(), r2.all());
    }

    #[test]
    fn rejects_duplicates_and_reserved() {
        let m = |id: u32, sym: &str| InstrumentMeta {
            id,
            venue: Venue::Kraken,
            venue_symbol: sym.to_string(),
            canonical: sym.to_string(),
        };
        assert_eq!(
            Registry::new(vec![m(1, "A/USD"), m(1, "B/USD")]).unwrap_err(),
            RegistryError::DuplicateId(1)
        );
        assert_eq!(
            Registry::new(vec![m(1, "A/USD"), m(2, "A/USD")]).unwrap_err(),
            RegistryError::DuplicateSymbol("A/USD".into())
        );
        assert_eq!(
            Registry::new(vec![m(0, "A/USD")]).unwrap_err(),
            RegistryError::ReservedId
        );
    }
}
