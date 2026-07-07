# flashbook

An HFT-style real-time market-data platform in Rust, built from scratch and
measured honestly: exchange feed handlers (Coinbase Exchange, Binance, Kraken),
a limit-order-book reconstruction engine, a custom columnar tick store, a
binary pub/sub fan-out, and a deterministic replay/backtest harness.

**Status: under construction.** This README fills in as components land and
are measured. Nothing here will ever claim a number that doesn't trace to a
committed raw result file in `bench/results/`.

- `DECISIONS.md` — every non-obvious choice, recorded when made.
- `LIMITATIONS.md` — what this is not.
- `BENCHMARKS.md` — measured numbers + full methodology (when they exist).
- `ops/` — goal, execution state, soak evidence.
