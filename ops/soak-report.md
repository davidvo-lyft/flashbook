# Soak report — generated, not hand-written

Generated 2026-07-09 14:19:13Z by ops/gen-soak-report.py from ops/soak/stats.jsonl (committed telemetry).

## Goal gates (G2)

- Continuous >= 24h: **NOT MET as specified: longest CONTINUOUS hole-free window 11.2h (span 25.1h with 18 sleep holes; the capture process itself never crashed — see cadence and restart lines)**
- Zero crashes: **MET (0 restarts)**
- >= 5M messages captured: **MET** (raw capture; tick-store ingest below)
- Gap detection/resequencing stats logged: **MET** (per-minute JSONL, 1068 total-lines)

## Window

- Start (wall): 2026-07-08 04:01:25Z  |  End: 2026-07-09 05:04:26Z  |  Span: 25.05 h
- Capture sessions observed in stats: 1 (1 = never restarted)
- Stats cadence: max inter-line gap 7287s; lines with >2min gap: 18 (0 = no dead/asleep periods)

## Totals (cumulative, current session)

| metric | value |
|---|---|
| msgs | 55,819,963 |
| bytes | 13,898,634,866 |
| events | 226,404,844 |
| gaps | 0 |
| resyncs | 0 |
| reconnects | 34 |
| fallbacks | 0 |
| parse_errors | 0 |
| rest_snaps | 561 |
| rss ceiling (MB) | 44 |
| restarts (watchdog) | 0 |
| raw segments on disk | 214 files, 2.81 GB |

## Per venue (cumulative)

| venue | msgs | events | gaps | resyncs | reconnects | fallbacks | parse_errors | rest_snaps |
|---|---|---|---|---|---|---|---|---|
| coinbase | 5,422,447 | 64,346,151 | 0 | 0 | 11 | 0 | 0 | 346 |
| binance | 8,591,065 | 52,998,453 | 0 | 0 | 10 | 0 | 0 | 215 |
| kraken | 41,806,451 | 109,060,240 | 0 | 0 | 13 | 0 | 0 | 0 |

## Full-corpus replay verification (replay-verify)

- records: 55,820,598; events: 226,404,844
- Kraken CRC32 oracle: **41,692,848 verified, 0 mismatches**, 0 skipped (unsynced windows)
- parse errors: 0; fallbacks: 0; torn tails: 0 (expected: <= restarts + live tail)
- determinism digests: events 928fae558177d6dc, books 731fd594dbf3d08c (double-replay asserted by --twice)

## Tick-store ingest of the corpus

- events in store: 226,404,844 (gate: >= 5,000,000 -> MET)
- store bytes: 2,062,280,436 (9.11 B/event; 6.83x smaller than raw JSON payloads)

## Method notes

- Counters are cumulative per capture session and emitted every 60s; a process restart resets them (sessions counted above), so cross-session totals are the sum of final lines per session — with zero restarts the last line IS the total.
- 'gaps' are venue-sequence discontinuities detected by the codecs (Binance U/u chain, Coinbase trade-id/heartbeat continuity); Kraken integrity is checksum-based (verified in replay), so its gap counter stays 0 by design.
- Memory ceiling is the max RSS the stats emitter ever observed (`ps -o rss=`).
