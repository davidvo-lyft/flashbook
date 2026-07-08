//! flashbook-bench: shared measurement machinery.
//!
//! Everything BENCHMARKS.md claims is produced by code in this crate and
//! written to `bench/results/*.json` — the committed raw evidence. Rules
//! embodied here:
//!
//! - percentiles come from raw sample arrays via the nearest-rank method
//!   (no interpolation, no fitting), with `n`, warmup and max always
//!   recorded alongside;
//! - every result file self-describes the host (CPU, memory, OS, rustc)
//!   and the benchmark config, so a number can never be quoted without its
//!   context;
//! - result files are written atomically (tmp + rename) and never
//!   overwritten silently — re-runs get fresh timestamped names unless the
//!   caller explicitly fixes the name.

pub mod loadgen;
pub mod percentile;
pub mod results;

pub use percentile::Percentiles;
pub use results::{HostInfo, ResultFile, write_result};
