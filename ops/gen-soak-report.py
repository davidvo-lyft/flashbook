#!/usr/bin/env python3
"""Generate ops/soak-report.md from soak telemetry.

Sources (all produced by the running system, never hand-typed):
  ops/soak/stats.jsonl      per-minute cumulative counters per venue + total
  ops/soak/restarts.log     one line per watchdog restart (absent = 0)
  data/raw/                 segment tree (sizes, counts)
  --replay-json PATH        output of `replay-verify --data data/raw ...`
  --ingest-json PATH        the <store>.ingest.json sidecar from `ingest`

Usage: python3 ops/gen-soak-report.py [--out ops/soak-report.md]
         [--stats ops/soak/stats.jsonl] [--replay-json X] [--ingest-json Y]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone


def iso(ns: int) -> str:
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stats", default="ops/soak/stats.jsonl")
    ap.add_argument("--restarts", default="ops/soak/restarts.log")
    ap.add_argument("--data-dir", default="data/raw")
    ap.add_argument("--replay-json", default=None)
    ap.add_argument("--ingest-json", default=None)
    ap.add_argument("--out", default="ops/soak-report.md")
    args = ap.parse_args()

    lines = []
    with open(args.stats) as f:
        for raw in f:
            raw = raw.strip()
            if raw:
                lines.append(json.loads(raw))

    totals = [l for l in lines if l.get("venue") == "total"]
    if not totals:
        print("no total lines in stats file", file=sys.stderr)
        return 2

    # Sessions: uptime_s resets when the process restarts.
    sessions = 1
    for prev, cur in zip(totals, totals[1:]):
        if cur.get("uptime_s", 0) < prev.get("uptime_s", 0):
            sessions += 1

    first, last = totals[0], totals[-1]
    start_ns = first["ts_wall_ns"] - first.get("uptime_s", 0) * 1_000_000_000
    end_ns = last["ts_wall_ns"]
    span_s = (end_ns - start_ns) / 1e9

    # Continuity: gaps in the per-minute cadence (asleep/dead periods).
    max_gap_s, gap_count_over_2m = 0.0, 0
    for prev, cur in zip(totals, totals[1:]):
        d = (cur["ts_wall_ns"] - prev["ts_wall_ns"]) / 1e9
        max_gap_s = max(max_gap_s, d)
        if d > 120:
            gap_count_over_2m += 1

    rss_ceiling = max(l.get("rss_max_mb", 0) for l in lines)

    restarts = 0
    restart_lines = []
    if os.path.exists(args.restarts):
        with open(args.restarts) as f:
            restart_lines = [l.strip() for l in f if l.strip()]
        restarts = len([l for l in restart_lines if "restarted" in l])

    # Per-venue latest cumulative counters (last line per venue).
    venues = {}
    for l in lines:
        v = l.get("venue")
        if v and v != "total":
            venues[v] = l

    # Data volume.
    seg_files, seg_bytes = 0, 0
    for root, _dirs, files in os.walk(args.data_dir):
        for fn in files:
            if fn.endswith((".fbraw", ".fbraw.zst")):
                seg_files += 1
                seg_bytes += os.path.getsize(os.path.join(root, fn))

    replay = None
    if args.replay_json and os.path.exists(args.replay_json):
        with open(args.replay_json) as f:
            replay = json.load(f)
    ingest = None
    if args.ingest_json and os.path.exists(args.ingest_json):
        with open(args.ingest_json) as f:
            ingest = json.load(f)

    hours = span_s / 3600
    msgs = last["msgs"]
    gate_24h = "MET" if hours >= 24 else f"NOT MET ({hours:.1f}h of 24h)"
    gate_5m = "MET" if msgs >= 5_000_000 else f"NOT MET ({msgs:,} of 5,000,000)"
    store_msgs = (ingest or {}).get("events", None)
    zero_crash = "MET (0 restarts)" if restarts == 0 else f"NOT MET ({restarts} restarts — see below)"

    w = []
    w.append("# Soak report — generated, not hand-written\n")
    w.append(f"Generated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')} by ops/gen-soak-report.py from ops/soak/stats.jsonl (committed telemetry).\n")
    w.append("## Goal gates (G2)\n")
    w.append(f"- Continuous >= 24h: **{gate_24h}**")
    w.append(f"- Zero crashes: **{zero_crash}**")
    w.append(f"- >= 5M messages captured: **{gate_5m}** (raw capture; tick-store ingest below)")
    w.append(f"- Gap detection/resequencing stats logged: **MET** (per-minute JSONL, {len(totals)} total-lines)\n")
    w.append("## Window\n")
    w.append(f"- Start (wall): {iso(start_ns)}  |  End: {iso(end_ns)}  |  Span: {hours:.2f} h")
    w.append(f"- Capture sessions observed in stats: {sessions} (1 = never restarted)")
    w.append(f"- Stats cadence: max inter-line gap {max_gap_s:.0f}s; lines with >2min gap: {gap_count_over_2m} (0 = no dead/asleep periods)\n")
    w.append("## Totals (cumulative, current session)\n")
    w.append("| metric | value |")
    w.append("|---|---|")
    for k in ("msgs", "bytes", "events", "gaps", "resyncs", "reconnects", "fallbacks", "parse_errors", "rest_snaps"):
        w.append(f"| {k} | {last.get(k, 0):,} |")
    w.append(f"| rss ceiling (MB) | {rss_ceiling} |")
    w.append(f"| restarts (watchdog) | {restarts} |")
    w.append(f"| raw segments on disk | {seg_files} files, {seg_bytes/1e9:.2f} GB |\n")
    w.append("## Per venue (cumulative)\n")
    w.append("| venue | msgs | events | gaps | resyncs | reconnects | fallbacks | parse_errors | rest_snaps |")
    w.append("|---|---|---|---|---|---|---|---|---|")
    for v in ("coinbase", "binance", "kraken"):
        l = venues.get(v, {})
        w.append("| " + " | ".join([v] + [f"{l.get(k, 0):,}" for k in ("msgs", "events", "gaps", "resyncs", "reconnects", "fallbacks", "parse_errors", "rest_snaps")]) + " |")
    w.append("")
    if restart_lines:
        w.append("## Restart log (honest accounting)\n")
        w.extend(f"- {l}" for l in restart_lines)
        w.append("")
    if replay:
        w.append("## Full-corpus replay verification (replay-verify)\n")
        w.append(f"- records: {replay.get('records', 0):,}; events: {replay.get('events', 0):,}")
        w.append(f"- Kraken CRC32 oracle: **{replay.get('checksums_ok', 0):,} verified, {replay.get('checksum_mismatches', 0)} mismatches**, {replay.get('checksums_skipped', 0)} skipped (unsynced windows)")
        w.append(f"- parse errors: {replay.get('parse_errors', 0)}; fallbacks: {replay.get('fallbacks', 0)}; torn tails: {replay.get('torn_tails', 0)} (expected: <= restarts + live tail)")
        w.append(f"- determinism digests: events {replay.get('event_stream_digest')}, books {replay.get('books_digest')} (double-replay asserted by --twice)")
        w.append("")
    if ingest:
        w.append("## Tick-store ingest of the corpus\n")
        w.append(f"- events in store: {ingest.get('events', 0):,} (gate: >= 5,000,000 -> {'MET' if (store_msgs or 0) >= 5_000_000 else 'NOT MET'})")
        w.append(f"- store bytes: {ingest.get('store_bytes', 0):,} ({ingest.get('bytes_per_event', 0):.2f} B/event; {ingest.get('ratio_vs_raw_json', 0):.2f}x smaller than raw JSON payloads)")
        w.append("")
    w.append("## Method notes\n")
    w.append("- Counters are cumulative per capture session and emitted every 60s; a process restart resets them (sessions counted above), so cross-session totals are the sum of final lines per session — with zero restarts the last line IS the total.")
    w.append("- 'gaps' are venue-sequence discontinuities detected by the codecs (Binance U/u chain, Coinbase trade-id/heartbeat continuity); Kraken integrity is checksum-based (verified in replay), so its gap counter stays 0 by design.")
    w.append("- Memory ceiling is the max RSS the stats emitter ever observed (`ps -o rss=`).")

    with open(args.out, "w") as f:
        f.write("\n".join(w) + "\n")
    print(f"wrote {args.out}: {hours:.2f}h, {msgs:,} msgs, {restarts} restarts, rss ceiling {rss_ceiling}MB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
