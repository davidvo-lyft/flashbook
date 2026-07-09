#!/usr/bin/env python3
"""Render BENCHMARKS.md from committed raw benchmark result files.

Every number in BENCHMARKS.md is rendered from `bench/results/*.json`
(the ResultFile schema written by crates/bench/src/results.rs); nothing is
hand-typed. Sections whose result files are absent render as pending.

Usage:
    python3 bench/render.py                       # print to stdout
    python3 bench/render.py --write               # write BENCHMARKS.md
    python3 bench/render.py --results-dir DIR --out FILE --write
    python3 bench/render.py --require feed_parse,lob_replay   # exit 2 if missing

Only top-level *.json files in the results dir are read (never tmp/).
Stdlib only. The footer carries a sha256 over the sorted concatenation of
input file bytes so drift between inputs and the rendered doc is detectable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------- formatting

def fmt_ns(v):
    """Auto-scale a nanosecond value to ns/µs/ms/s."""
    if v is None:
        return "—"
    v = float(v)
    for div, unit, prec in ((1e9, "s", 3), (1e6, "ms", 2), (1e3, "µs", 2)):
        if v >= div:
            return f"{v / div:.{prec}f} {unit}"
    return f"{v:.0f} ns"


def fmt_ns_pair(v):
    """Scaled value plus raw ns in parens (used for p50/p99 at least)."""
    if v is None:
        return "—"
    s = fmt_ns(v)
    return s if s.endswith(" ns") else f"{s} ({int(v):,} ns)"


def fmt_rate(v):
    """Humanize a per-second rate."""
    if v is None:
        return "—"
    v = float(v)
    for div, unit, prec in ((1e9, "G", 2), (1e6, "M", 2), (1e3, "k", 1)):
        if v >= div:
            return f"{v / div:.{prec}f} {unit}/s"
    return f"{v:.0f} /s"


def fmt_bytes(v):
    if v is None:
        return "—"
    for unit, div in (("GiB", 2**30), ("MiB", 2**20), ("KiB", 2**10)):
        if float(v) >= div:
            return f"{v / div:.2f} {unit}"
    return f"{int(v)} B"


def fmt_secs(v):
    return "—" if v is None else (f"{v * 1e3:.1f} ms" if v < 1.0 else f"{v:.3f} s")


def fmt_x(v):
    return "—" if v is None else f"{v:.2f}×"


def fmt_frac(v):
    """Small per-frame floats (alloc counts): exact 0 stays '0'."""
    return "0" if v == 0 else f"{v:.4f}".rstrip("0").rstrip(".")


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |",
           "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    out.append("")
    return out


PCTL_HDR = ["n", "min", "p50", "p90", "p99", "p999", "max", "mean"]


def pctl_cells(p):
    """Cells for one {n,min,p50,p90,p99,p999,max,mean,stdev} object (ns)."""
    if not isinstance(p, dict):
        return ["—"] * len(PCTL_HDR)
    return [f"{p.get('n', 0):,}", fmt_ns(p.get("min")), fmt_ns_pair(p.get("p50")),
            fmt_ns(p.get("p90")), fmt_ns_pair(p.get("p99")), fmt_ns(p.get("p999")),
            fmt_ns(p.get("max")), fmt_ns(p.get("mean"))]


def bold_best(vals, fmt, better=min):
    """Format a row of comparable values, bolding the best present one
    (wins AND losses rendered plainly, whoever's they are)."""
    present = [v for v in vals if v is not None]
    best = better(present) if present else None
    return ["—" if v is None else (f"**{fmt(v)}**" if v == best else fmt(v))
            for v in vals]


def src(*names):
    return "*src: " + ", ".join(f"`{n}.json`" for n in names) + "*"


def pending(*names):
    return ["*pending — no result file (" + ", ".join(f"`{n}.json`" for n in names) + ")*", ""]


def quick_badge(d):
    cfg = d.get("config") or {}
    quick = isinstance(cfg, dict) and cfg.get("quick")
    if quick or "QUICK" in str(d.get("notes", "")):
        return " — **QUICK/smoke run, not official numbers**"
    return ""


def notes_quote(d):
    n = str(d.get("notes") or "").strip().replace("\n", " ")
    return ["> **Producer notes (verbatim):** " + n, ""] if n else []


# ------------------------------------------------------------------- loading

def load_results(results_dir: Path):
    """Load every top-level *.json (never tmp/). Returns (files, blobs, warnings)."""
    files, blobs, warnings = {}, {}, []
    if not results_dir.is_dir():
        warnings.append(f"results dir `{results_dir}` does not exist — rendering all-pending")
        return files, blobs, warnings
    for p in sorted(results_dir.glob("*.json")):
        if not p.is_file() or p.name.startswith("."):
            continue
        raw = p.read_bytes()
        blobs[p.name] = raw
        try:
            d = json.loads(raw)
        except ValueError as e:
            warnings.append(f"`{p.name}`: unparsable JSON, skipped ({e})")
            continue
        name = d.get("name", p.stem)
        if name != p.stem:
            warnings.append(f"`{p.name}`: embedded name `{name}` != file stem `{p.stem}`")
        if d.get("schema") != 1:
            warnings.append(f"`{p.name}`: schema {d.get('schema')!r} != 1 — renderer targets schema 1")
        files[p.stem] = d
    return files, blobs, warnings


# ------------------------------------------------------------------ sections

PREAMBLE = """\
# BENCHMARKS

**Generated file — do not edit numbers by hand.** Every number below is
rendered from a committed raw result file in `bench/results/*.json` (schema:
`crates/bench/src/results.rs`) by `bench/render.py`; a number that is not
traceable to a result file does not get written here — by construction.
Each table cites its source file inline.

## Methodology (applies to every section)

- **Hardware/OS**: recorded per-run in each result file (`host` field) and
  tabulated below; inconsistencies between result files are flagged.
- **This is a laptop-class SoC on macOS**, not a tuned Linux box: no
  io_uring, no isolcpus, no IRQ steering. Isolation steps actually taken per
  run: AC power, foreground apps closed, machine otherwise idle, cold-start
  (no prior thermal load); these are stated, their limits acknowledged —
  p999/max include macOS scheduler noise. See LIMITATIONS.md.
- **Percentiles**: nearest-rank over raw sample arrays (`P(q) =
  sorted[ceil(q*n)-1]`), never interpolated or fitted. `n`, warmup count,
  mean, stdev, and max are always published alongside. With small `n`, high
  percentiles saturate at max — reported as such, not extrapolated.
- **Warmup**: stated per benchmark; warmup samples are discarded and counted.
- **Real data**: parse/LOB/store benchmarks run over the captured soak
  corpus (see ops/soak-report.md), not synthetic JSON, unless a section
  explicitly says otherwise (bus benchmarks use the seeded deterministic
  generator to isolate transport cost).
- **Baselines are real implementations**, not strawmen: the serde_json
  baseline is the actual `parse_slow` production fallback path; DuckDB and
  SQLite comparisons use their bundled current releases with stated schemas,
  indexes and pragmas, on identical data.
- **Losses are published.** Where an off-the-shelf engine beats this code,
  the table says so (best value per row is bolded, whoever's it is).
"""


def sec_provenance(files, warnings):
    L = ["## Host & provenance", ""]
    if not files:
        L += ["*No result files found — every section below is pending.*", ""]
        return L
    rows = []
    for stem, d in files.items():
        h = d.get("host") or {}
        created = d.get("created_unix_ms")
        ts = (datetime.fromtimestamp(created / 1000, tz=timezone.utc)
              .strftime("%Y-%m-%d %H:%M:%SZ") if created else "—")
        cfg = d.get("config") or {}
        quick = "yes" if (isinstance(cfg, dict) and cfg.get("quick")) else \
                ("yes (notes)" if "QUICK" in str(d.get("notes", "")) else "—")
        rows.append([f"`{stem}.json`", ts, h.get("cpu", "—"), h.get("cores", "—"),
                     h.get("mem_gb", "—"), h.get("os", "—"), h.get("rustc", "—"), quick])
    L += md_table(["file", "created (UTC)", "cpu", "cores", "mem (GB)", "os", "rustc", "quick"], rows)
    hosted = {k: d for k, d in files.items() if isinstance(d.get("host"), dict)}
    for k in files:
        if k not in hosted:
            L.append(f"- **FLAGGED:** `{k}.json` has no `host` object — not a ResultFile; "
                     "excluded from host-consistency checks.")
    for field in ("cpu", "cores", "mem_gb", "os", "rustc"):
        vals = sorted({repr(d["host"].get(field)) for d in hosted.values()})
        if len(vals) > 1:
            L.append(f"- **FLAGGED — `host.{field}` inconsistent across result files:** {', '.join(vals)}")
    quick_n = sum(1 for d in files.values()
                  if (isinstance(d.get("config"), dict) and d["config"].get("quick"))
                  or "QUICK" in str(d.get("notes", "")))
    if quick_n:
        L.append(f"- **WARNING: {quick_n} of {len(files)} input files are QUICK/smoke runs — "
                 "numbers below are not official until replaced by full runs.**")
    for w in warnings:
        L.append(f"- **FLAGGED:** {w}")
    L.append("")
    return L


def sec_feed(F):
    L = ["## Feed — JSON→Event normalization (3a)", ""]
    fp = F.get("feed_parse")
    if not fp:
        L += pending("feed_parse")
    else:
        m = fp["metrics"]
        L += [f"### Throughput: fast scanner vs serde_json baseline{quick_badge(fp)}", ""]
        rows = []
        for v in m.get("venues", []):
            rows.append([v["venue"], f"{v.get('ws_frames', 0):,}",
                         fmt_rate(v["fast"]["msgs_per_s"]), fmt_rate(v["slow"]["msgs_per_s"]),
                         fmt_x(v.get("fast_over_slow")),
                         fmt_bytes(v["fast"].get("bytes_per_s")) + "/s",
                         f"{v['fast'].get('fallbacks', 0):,}", f"{v['fast'].get('parse_errors', 0):,}"])
        agg = m.get("aggregate", {})
        venues = m.get("venues", [])
        rows.append(["**aggregate**", f"{sum(v.get('ws_frames', 0) for v in venues):,}",
                     f"**{fmt_rate(agg.get('fast_msgs_per_s'))}**", fmt_rate(agg.get("slow_msgs_per_s")),
                     f"**{fmt_x(agg.get('fast_over_slow'))}**",
                     fmt_bytes(agg.get("fast_bytes_per_s")) + "/s",
                     f"{sum(v['fast'].get('fallbacks', 0) for v in venues):,}",
                     f"{sum(v['fast'].get('parse_errors', 0) for v in venues):,}"])
        L += md_table(["venue", "ws frames", "fast msgs/s", "slow msgs/s", "fast/slow",
                       "fast bytes/s", "fallbacks", "parse errors"], rows)
        L += [f"Aggregate fast path emits normalized events at {fmt_rate(agg.get('fast_events_per_s'))}. "
              + src("feed_parse"), "", *notes_quote(fp)]
    fa = F.get("feed_alloc")
    if not fa:
        L += pending("feed_alloc")
    else:
        L += [f"### Allocations per frame (dhat){quick_badge(fa)}", ""]
        rows, zero, nonzero = [], [], []
        for v in fa["metrics"].get("venues", []):
            fast, slow = v["paths"].get("fast", {}), v["paths"].get("slow", {})
            rows.append([v["venue"], fmt_frac(fast.get("blocks_per_frame")), fmt_frac(fast.get("bytes_per_frame")),
                         fmt_frac(slow.get("blocks_per_frame")), fmt_frac(slow.get("bytes_per_frame")),
                         f"{fast.get('rest_snapshots', 0):,}", f"{fast.get('ws_frames', 0):,}"])
            if fast.get("total_blocks") == 0 and fast.get("total_bytes") == 0:
                zero.append(f"{v['venue']} ({fast.get('ws_frames', 0):,} frames, 0 blocks, 0 bytes)")
            else:
                nonzero.append(f"{v['venue']} ({fast.get('total_blocks', 0):,} blocks / "
                               f"{fast.get('total_bytes', 0):,} B over {fast.get('ws_frames', 0):,} frames)")
        L += md_table(["venue", "fast allocs/frame", "fast bytes/frame", "slow allocs/frame",
                       "slow bytes/frame", "REST snaps (fast)", "frames"], rows)
        if zero:
            L.append("**Zero allocations/frame measured** (exactly 0 in this run) for the fast path on: "
                     + "; ".join(zero) + ". " + src("feed_alloc"))
        if nonzero:
            L.append(("Fast path is **not** zero-allocation for: " if zero else
                      "No path measured zero allocations. Fast path allocations: ")
                     + "; ".join(nonzero) + " — see producer notes for attribution. " + src("feed_alloc"))
        L += ["", *notes_quote(fa)]
    return L


def sec_lob(F):
    L = ["## LOB — book replay & top-of-book latency (3b)", ""]
    lr = F.get("lob_replay")
    if not lr:
        L += pending("lob_replay")
        return L
    m, cfg = lr["metrics"], lr.get("config", {})
    reps = sorted(k for k, v in m.items() if isinstance(v, dict) and "mean_events_per_s" in v)
    L += [f"### Replay throughput per representation{quick_badge(lr)}", "",
          f"Corpus: `{cfg.get('data', '?')}` — {cfg.get('records', 0):,} records / "
          f"{cfg.get('events', 0):,} events / {cfg.get('ws_frames', 0):,} WS frames; "
          f"checksums ok {cfg.get('checksums_ok', 0):,}, mismatches {cfg.get('checksum_mismatches', 0):,}; "
          f"{cfg.get('warmup_passes', '?')} warmup + {cfg.get('measured_passes', '?')} measured passes, "
          f"{cfg.get('threads', '?')} thread(s). " + src("lob_replay"), ""]
    winner_by_means = max(reps, key=lambda r: m[r]["mean_events_per_s"]) if reps else None
    rows = [[r, ", ".join(fmt_rate(x) for x in m[r].get("events_per_s", [])),
             f"**{fmt_rate(m[r]['mean_events_per_s'])}**" if r == winner_by_means
             else fmt_rate(m[r]["mean_events_per_s"]),
             ", ".join(f"{s:.4g} s" for s in m[r].get("pass_seconds", []))] for r in reps]
    L += md_table(["representation", "events/s per pass", "mean events/s", "pass seconds"], rows)
    L.append(f"**Winner (declared by the data, mean events/s): `{winner_by_means}`.** "
             f"Result file's own `winner` field: `{m.get('winner')}`."
             + ("" if m.get("winner") == winner_by_means else
                " **FLAGGED: `winner` field disagrees with the means above.**"))
    dm = m.get("digests_match")
    L.append(f"Differential check: `digests_match = {str(dm).lower()}`"
             + (" — all representations produced identical end-state digests."
                if dm else " — **WARNING: representations diverged.**"))
    L += ["", "### Top-of-book update latency", ""]
    tob = m.get("tob_latency", {})
    L += md_table(["representation"] + PCTL_HDR,
                  [[r] + pctl_cells(tob[r]) for r in sorted(tob)])
    to = m.get("timer_overhead_ns")
    if to is not None:
        L.append(f"Timer-overhead calibration: each sample includes one `Instant::now()/elapsed()` pair, "
                 f"measured at {to:.1f} ns on this run (published, not subtracted). " + src("lob_replay"))
    L += ["", *notes_quote(lr)]
    return L


def sec_store(F):
    L = ["## Store — write, scan, point-in-time, head-to-head (3c)", ""]
    sw = F.get("store_write")
    if not sw:
        L += pending("store_write")
    else:
        L += [f"### Write throughput (encode only){quick_badge(sw)}", ""]
        m = sw["metrics"]
        rows = [[mode, fmt_rate(m[mode].get("mean_events_per_s")),
                 f"{m[mode].get('mean_mb_per_s_logical', 0):,.0f} MB/s",
                 f"{m[mode].get('bytes_per_event', 0):.2f} B",
                 f"{fmt_bytes(m[mode].get('stored_bytes'))} ({m[mode].get('stored_bytes', 0):,} B)",
                 m[mode].get("zstd_level") if m[mode].get("zstd_level") is not None else "—"]
                for mode in sorted(m) if isinstance(m[mode], dict)]
        L += md_table(["mode", "events/s", "logical MB/s", "bytes/event", "stored bytes", "zstd level"], rows)
        L += [src("store_write"), "", *notes_quote(sw)]
    ss = F.get("store_scan")
    if not ss:
        L += pending("store_scan")
    else:
        m = ss["metrics"]
        L += [f"### Full-scan throughput{quick_badge(ss)}", ""]
        rows = [[f"pass {i + 1}", f"{s:.4g} s", fmt_rate(m['events_per_s'][i]),
                 f"{m['gb_per_s_logical'][i]:.3g} GB/s", f"{m['gb_per_s_physical'][i]:.3g} GB/s"]
                for i, s in enumerate(m.get("pass_seconds", []))]
        rows.append(["**mean**", "—", f"**{fmt_rate(m.get('mean_events_per_s'))}**",
                     f"**{m.get('mean_gb_per_s_logical', 0):.3g} GB/s**",
                     f"**{m.get('mean_gb_per_s_physical', 0):.3g} GB/s**"])
        L += md_table(["pass", "seconds", "events/s", "logical GB/s", "physical GB/s"], rows)
        L += [f"{m.get('events', 0):,} events scanned. " + src("store_scan"), "", *notes_quote(ss)]
    sp = F.get("store_pit")
    if not sp:
        L += pending("store_pit")
    else:
        m = sp["metrics"]
        L += [f"### Point-in-time snapshot query latency{quick_badge(sp)}", ""]
        L += md_table(["queries"] + PCTL_HDR,
                      [[f"{m.get('queries', 0):,}"] + pctl_cells(m.get("latency_ns"))])
        L += [f"Anchor hit rate {m.get('anchor_hit_rate', 0):.0%} ({m.get('anchor_hits', 0):,}/"
              f"{m.get('queries', 0):,}); {m.get('snapshots_indexed', 0):,} snapshots indexed "
              f"(index source: {m.get('index_source', '?')}). Misses are timed as the near-free "
              "lookups they are. " + src("store_pit"), "", *notes_quote(sp)]
    sc = F.get("store_compare")
    if not sc:
        L += pending("store_compare")
    else:
        m, cfg = sc["metrics"], sc.get("config", {})
        events = cfg.get("events") or 0
        sizes, load = m.get("sizes_bytes", {}), m.get("load_seconds", {})
        scan, pit = m.get("full_scan_seconds", {}), m.get("pit_latency_ns", {})
        raw_json = sizes.get("raw_json")
        L += [f"### Head-to-head: ours vs DuckDB vs SQLite vs Parquet-zstd{quick_badge(sc)}", "",
              f"Identical {events:,} events in every backend. Raw-JSON baseline "
              f"{fmt_bytes(raw_json)} ({raw_json:,} B) from `metrics.sizes_bytes.raw_json`. "
              "Best value per row is **bolded regardless of whose it is**; — means the backend "
              "has no such measurement in the result file. " + src("store_compare"), ""]
        cols = ["ours (fbstore)", "DuckDB", "SQLite", "Parquet-zstd"]
        size_vals = [sizes.get("ours_total"), sizes.get("duckdb"), sizes.get("sqlite"), sizes.get("parquet_zstd")]
        per_ev = [None if s is None or not events else s / events for s in size_vals]
        ratio = [None if s in (None, 0) or raw_json is None else raw_json / s for s in size_vals]
        p50s = [pit.get(k, {}).get("p50") for k in ("ours", "duckdb", "sqlite")] + [None]
        p99s = [pit.get(k, {}).get("p99") for k in ("ours", "duckdb", "sqlite")] + [None]
        table_rows = [
            (["load seconds"] + bold_best([None, load.get("duckdb_appender"),
                                           load.get("sqlite_tx_insert_index_analyze"),
                                           m.get("parquet_write_seconds")], fmt_secs) + ["lower"]),
            (["on-disk bytes"] + bold_best(size_vals, lambda v: f"{fmt_bytes(v)} ({int(v):,} B)") + ["lower"]),
            (["bytes/event"] + bold_best(per_ev, lambda v: f"{v:.2f} B") + ["lower"]),
            (["ratio vs raw JSON"] + bold_best(
                ratio, lambda v: f"{v:.2f}× smaller" if v >= 1 else f"{v:.2f}× — LARGER than raw JSON",
                better=max) + ["higher"]),
            (["full-scan seconds"] + bold_best([scan.get("mean_ours"), scan.get("mean_duckdb"),
                                                scan.get("mean_sqlite"), None], fmt_secs) + ["lower"]),
            (["PIT p50"] + bold_best(p50s, fmt_ns_pair) + ["lower"]),
            (["PIT p99"] + bold_best(p99s, fmt_ns_pair) + ["lower"]),
        ]
        L += md_table(["metric"] + cols + ["better"], table_rows)
        L += ["Notes on blanks: ours' load is the capture-time ingest (see `store_write.json` for encode "
              "cost); Parquet is written via DuckDB COPY and has no scan/PIT harness in this result file.", ""]
        L += ["Full PIT latency percentiles per backend:", ""]
        L += md_table(["backend"] + PCTL_HDR, [[k] + pctl_cells(pit[k]) for k in sorted(pit)])
        par, winners = m.get("parity", {}), m.get("winners", {})
        L.append(f"Parity before timing: full_scan_equal={str(par.get('full_scan_equal')).lower()}, "
                 f"pit_tops_equal={str(par.get('pit_tops_equal')).lower()}, "
                 f"anchor hits {par.get('pit_anchor_hits', 0)}/{par.get('pit_queries', 0)}, "
                 f"divergences {par.get('pit_anchor_divergences', 0)}, "
                 f"failures {par.get('failures', [])}.")
        if winners:
            L.append("Result file's own `winners` field (wins and losses, plainly): "
                     + ", ".join(f"{k} → **{v}**" for k, v in sorted(winners.items())) + ".")
        L += ["", *notes_quote(sc)]
    return L


BUS_NAMES = {"ring": "ring (seqlock, ours)", "crossbeam_channel": "crossbeam-channel fan-out",
             "tokio_broadcast": "tokio::broadcast"}


def sec_bus(F):
    L = ["## Bus — in-process fan-out & loopback network (3d)", ""]
    bf = F.get("bus_fanout")
    if not bf:
        L += pending("bus_fanout")
    else:
        m = bf["metrics"]
        contenders = [c for c in ("ring", "crossbeam_channel", "tokio_broadcast") if c in m] \
            + sorted(k for k in m if isinstance(m[k], dict) and k not in BUS_NAMES)
        L += [f"### Fan-out throughput over subscriber counts{quick_badge(bf)}", ""]
        rows = []
        for c in contenders:
            for row in m[c].get("throughput", []):
                per = row.get("per_subscriber", [])
                lost = sum(p.get("lost", 0) for p in per)
                del_rates = [p.get("effective_delivery_msgs_per_s", 0) for p in per]
                deliv = fmt_rate(del_rates[0]) if len(del_rates) == 1 else \
                    f"{fmt_rate(min(del_rates))} – {fmt_rate(max(del_rates))}"
                expected = row.get("msgs", 0) * max(len(per), 1)
                lost_s = "0" if lost == 0 else f"**{lost:,} ({lost / expected:.1%})**"
                rows.append([BUS_NAMES.get(c, c), row.get("subscribers"),
                             fmt_rate(row.get("producer_publish_msgs_per_s")), deliv, lost_s])
        L += md_table(["contender", "subs", "publish rate", "delivery rate/sub (min–max)", "lost"], rows)
        L += [src("bus_fanout"), "", f"### Fan-out delivery latency (paced publisher){quick_badge(bf)}", ""]
        rows = []
        for c in contenders:
            for row in m[c].get("latency", []):
                lost = sum(p.get("lost", 0) for p in row.get("per_subscriber", []))
                lat = row.get("latency_ns", {})
                rows.append([BUS_NAMES.get(c, c), row.get("subscribers"),
                             fmt_rate(row.get("achieved_publish_msgs_per_s")),
                             "0" if lost == 0 else f"**{lost:,}**",
                             fmt_ns_pair(lat.get("p50")), fmt_ns(lat.get("p90")),
                             fmt_ns_pair(lat.get("p99")), fmt_ns(lat.get("p999")),
                             fmt_ns(lat.get("max")), f"{lat.get('n', 0):,}"])
        L += md_table(["contender", "subs", "achieved pub rate", "lost", "p50", "p90",
                       "p99", "p999", "max", "n"], rows)
        L += [src("bus_fanout"), "", *notes_quote(bf)]
    en = F.get("e2e_net")
    if not en:
        L += pending("e2e_net")
    else:
        m, cfg = en["metrics"], en.get("config", {})
        L += [f"### Loopback TCP fan-out (e2e_net){quick_badge(en)}", "",
              "> **LOOPBACK IS NOT A NIC.** This measures kernel network-stack + syscall + "
              "scheduler-handoff cost on one host: no wire serialization, no propagation, no NIC "
              "interrupt/coalescing behavior. Treat as a floor for cross-machine fan-out latency. "
              "(Caveat restated from the result file's own notes, quoted in full below.)", ""]
        sustained = m.get("sustained")
        flag = (f"**Sustained: yes** — achieved {fmt_rate(m.get('achieved_rate_per_sec'))} vs target "
                f"{fmt_rate(cfg.get('target_rate_per_sec'))}" if sustained else
                f"**Sustained: NO — the pacing schedule slipped; the ACHIEVED rate "
                f"{fmt_rate(m.get('achieved_rate_per_sec'))} is what the latencies below were measured at, "
                f"not the {fmt_rate(cfg.get('target_rate_per_sec'))} target.**")
        L += [flag + f" ({cfg.get('subs', '?')} subscribers, {cfg.get('events', 0):,} events, "
              f"elapsed {fmt_ns(m.get('elapsed_ns'))}). " + src("e2e_net"), ""]
        rows = [["merged (all subs)", "—"] + pctl_cells(m.get("merged_latency_ns"))]
        rows += [[f"sub {i}", f"{s.get('delivered', 0):,}"] + pctl_cells(s.get("latency_ns"))
                 for i, s in enumerate(m.get("per_sub", []))]
        L += md_table(["stream", "delivered"] + PCTL_HDR, rows)
        L += [src("e2e_net"), "", *notes_quote(en)]
    return L


E2E_STAGES = [("parse", "parse_ns"), ("publish", "publish_ns"), ("deliver", "deliver_ns"),
              ("deliver (steady)", "deliver_steady_ns"), ("total added", "total_added_ns"),
              ("total added (steady)", "total_added_steady_ns")]


def _stage_table(stages):
    rows = [[label] + pctl_cells(stages[key]) for label, key in E2E_STAGES if key in stages]
    return md_table(["stage"] + PCTL_HDR, rows)


def sec_e2e(F):
    L = ["## E2E — exchange→subscriber added latency (3e)", ""]
    el = F.get("e2e_live")
    if not el:
        L += pending("e2e_live")
    else:
        m = el["metrics"]
        venues = m.get("venues", {})
        L += [f"### Local pipeline decomposition on live venue traffic{quick_badge(el)}", "",
              f"{m.get('venues_connected', 0)} venue(s) connected. `total added` starts at socket read "
              "and contains zero internet time by construction; `(steady)` rows exclude initial-snapshot "
              "drain events and are the steady-state numbers (see producer notes). " + src("e2e_live"), "",
              "**Aggregate (all venues)**", ""]
        L += _stage_table(m.get("aggregate", {}))
        for v in sorted(venues):
            L += [f"**{v}**", ""]
            L += _stage_table(venues[v].get("stages", {}))
        L += ["Per-venue counters:", ""]
        rows = [[v, "yes" if d.get("connected") else "**NO**", f"{d.get('frames', 0):,}",
                 f"{d.get('events', 0):,}", d.get("fallbacks", 0), d.get("parse_errors", 0),
                 d.get("resync_signals", 0), d.get("lagged_lost", 0), d.get("unmatched_deliver", 0)]
                for v, d in sorted(venues.items())]
        L += md_table(["venue", "connected", "frames", "events", "fallbacks", "parse errors",
                       "resync signals", "lagged lost", "unmatched deliver"], rows)
        for v, d in sorted(venues.items()):
            if (d.get("note") or "").strip():
                L.append(f"- venue note ({v}): {d['note'].strip()}")
        L += [src("e2e_live"), "",
              "### Venue path — context only, **NOT flashbook's added latency**", "",
              "`venue_path` = venue-side batching + WAN transit + venue↔host wall-clock offset; "
              "it is published as context and is not attributable to this code.", ""]
        rows = []
        for v, d in sorted(venues.items()):
            vp = d.get("venue_path_ns", {})
            clamped, n = d.get("venue_path_clamped", 0), vp.get("n", 0)
            warn = " **(mostly clamped — uninterpretable; use RTT file)**" if n and clamped >= n / 2 else ""
            rows.append([v] + pctl_cells(vp) + [f"{clamped:,}{warn}"])
        L += md_table(["venue"] + PCTL_HDR + ["clamped to 0"], rows)
        L += [src("e2e_live"), "", *notes_quote(el)]
    er = F.get("e2e_rtt")
    if not er:
        L += pending("e2e_rtt")
    else:
        m = er["metrics"]
        L += [f"### Internet RTT per venue (WS ping/pong){quick_badge(er)}", ""]
        rows = [[v, d.get("pings_sent", 0), d.get("pongs_matched", 0)] + pctl_cells(d.get("rtt_ns"))
                for v, d in sorted(m.get("venues", {}).items())]
        L += md_table(["venue", "pings", "pongs"] + PCTL_HDR, rows)
        for v, d in sorted(m.get("venues", {}).items()):
            if (d.get("note") or "").strip():
                L.append(f"- venue note ({v}): {d['note'].strip()}")
        L += [src("e2e_rtt") + " — small n by design; high percentiles saturate at max.", ""]
        L += ["Subtraction method, quoted from the result file notes:", *notes_quote(er)]
    return L


def sec_other(files):
    known = {"feed_parse", "feed_alloc", "lob_replay", "store_write", "store_scan",
             "store_pit", "store_compare", "bus_fanout", "e2e_net", "e2e_live", "e2e_rtt"}
    other = {k: v for k, v in files.items() if k not in known}
    if not other:
        return []
    L = ["## Other result files (no dedicated renderer — listed so nothing committed is invisible)", ""]
    for k, d in sorted(other.items()):
        if isinstance(d.get("metrics"), dict):
            desc = "metrics keys: " + ", ".join(sorted(d["metrics"]))
        elif isinstance(d, dict):
            desc = "not a ResultFile; top-level keys: " + ", ".join(sorted(d))
        else:
            desc = "not a JSON object"
        notes = str(d.get("notes") or "").strip() if isinstance(d, dict) else ""
        L.append(f"- `{k}.json` — {desc}." + (f" Notes: {notes[:300]}" if notes else ""))
    L.append("")
    return L


def footer(blobs, results_dir):
    h = hashlib.sha256()
    for name in sorted(blobs):
        h.update(blobs[name])
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    repo_root = Path(__file__).resolve().parent.parent
    try:  # keep the committed doc free of machine-specific absolute paths
        shown_dir = Path(results_dir).resolve().relative_to(repo_root)
    except ValueError:
        shown_dir = results_dir
    return ["---", "",
            "Regenerate: `bash bench/render.sh --write` (after `./bench/run-all.sh`).", "",
            f"Generated {now} from {len(blobs)} result file(s) in `{shown_dir}`: "
            + (", ".join(f"`{n}`" for n in sorted(blobs)) if blobs else "(none)") + ".",
            f"Inputs sha256 (sorted concatenation of input file bytes): `{h.hexdigest()}`.", ""]


def render(files, blobs, warnings, results_dir):
    lines = [PREAMBLE]
    for sec in (sec_provenance(files, warnings), sec_feed(files), sec_lob(files),
                sec_store(files), sec_bus(files), sec_e2e(files), sec_other(files),
                footer(blobs, results_dir)):
        lines += sec
    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------- main

def main(argv=None):
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description="Render BENCHMARKS.md from bench/results/*.json")
    ap.add_argument("--results-dir", default=str(script_dir / "results"),
                    help="directory of result files (top-level *.json only; tmp/ is never read)")
    ap.add_argument("--out", default=str(script_dir.parent / "BENCHMARKS.md"),
                    help="output path (only written with --write)")
    ap.add_argument("--write", action="store_true",
                    help="write --out; without this, print to stdout and write nothing")
    ap.add_argument("--require", default="",
                    help="comma-separated result names that must be present; exit 2 listing any missing")
    args = ap.parse_args(argv)

    results_dir = Path(args.results_dir)
    files, blobs, warnings = load_results(results_dir)
    for w in warnings:
        print(f"warning: {w}", file=sys.stderr)

    if args.require:
        required = [r.strip().removesuffix(".json") for r in args.require.split(",") if r.strip()]
        missing = [r for r in required if r not in files]
        if missing:
            print(f"missing required result files in {results_dir}: "
                  + ", ".join(f"{r}.json" for r in missing), file=sys.stderr)
            return 2

    md = render(files, blobs, warnings, results_dir)
    if args.write:
        out = Path(args.out)
        out.write_text(md, encoding="utf-8")
        print(f"wrote {out} ({len(md):,} bytes, {len(blobs)} input file(s))", file=sys.stderr)
    else:
        sys.stdout.write(md)
    return 0


if __name__ == "__main__":
    sys.exit(main())
