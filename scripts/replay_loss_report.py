#!/usr/bin/env python3
"""Report, minute-by-minute, where a Global Replay service may be losing messages.

For a given topic the script scans SCGRep log files and, for each one-minute
pub-time window, counts:

  * baseline messages  -- received live from the Global Brokers
                          ("Global Broker message: ... time=<pubtime>")
  * replayed messages  -- returned by the Global Replay service
                          ("Replay message (synchronous|asynchronous): ...")

and reports the difference (baseline - replay). A positive difference means the
replay service returned fewer messages than were seen live for that window, i.e.
messages that may have been lost by the replay service. The output is a table
plus a simple ASCII histogram of the difference against the pub-time window.

Both baseline and replay counts are de-duplicated by message ``id`` within each
minute, so a message delivered by several replay brokers (or paged more than
once) is counted once. Messages are bucketed by their pub-time (the ``time=``
field), the same clock the replay service filters on.

Counts are only meaningful for windows that SCGRep has actually replay-tested
(roughly ``TIME_LAG`` behind now), so by default the reporting window ends at the
most recent replayed message found in the logs rather than at the wall clock.

Everything is standard-library only; run with ``-h`` / ``--help`` for usage.
"""

from __future__ import annotations

import argparse
import glob
import gzip
import re
import shutil
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# --- log line patterns -------------------------------------------------------
BASELINE_RE = re.compile(
    r"Global Broker message: topic=(?P<topic>\S+) id=(?P<id>\S+) time=(?P<time>\S+)"
)
REPLAY_RE = re.compile(
    r"Replay message \((?P<kind>synchronous|asynchronous)\): "
    r"centre_id=\S+ topic=(?P<topic>\S+) id=(?P<id>\S+) time=(?P<time>\S+)"
)

# Per-cycle summary lines (the values SCGRep publishes to Prometheus / Grafana):
# the tested window, and the per-(centre, topic, protocol) baseline vs fetched.
TESTPERIOD_RE = re.compile(r"Test period begins: window (?P<start>\S+) \.\. (?P<end>\S+)")
RESULT_RE = re.compile(
    r"Result: centre_id=(?P<centre>\S+) topic=(?P<topic>\S+) protocol=(?P<protocol>\S+) "
    r"baseline=(?P<baseline>\d+) fetched=(?P<fetched>\d+)"
)

# --source values selecting which replayed *per-message* lines to count. The
# additional value "summary" switches to the per-cycle summary lines instead.
SOURCES = {
    "both": {"synchronous", "asynchronous"},
    "sync": {"synchronous"},
    "async": {"asynchronous"},
}


def parse_pubtime(value: str) -> datetime | None:
    """Parse an ISO-8601 pub-time (with or without ``Z``/fractional seconds) to
    an aware UTC datetime, or ``None`` if it cannot be parsed."""
    text = value.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def iter_log_lines(paths: list[str]):
    """Yield lines from each log file (transparently handling ``.gz``)."""
    for path in paths:
        opener = gzip.open if path.endswith(".gz") else open
        try:
            with opener(path, "rt", errors="replace") as handle:
                yield from handle
        except FileNotFoundError:
            print(f"warning: log file not found: {path}", file=sys.stderr)


def scan(lines, topic: str, sources: set[str]):
    """Scan log lines, returning ``(baseline, replay)`` dicts mapping each
    minute-bucket datetime to the set of unique message ids in that minute."""
    baseline: dict[datetime, set[str]] = defaultdict(set)
    replay: dict[datetime, set[str]] = defaultdict(set)
    for line in lines:
        match = BASELINE_RE.search(line)
        if match:
            if topic in match["topic"]:
                dt = parse_pubtime(match["time"])
                if dt is not None:
                    baseline[floor_minute(dt)].add(match["id"])
            continue
        match = REPLAY_RE.search(line)
        if match and match["kind"] in sources and topic in match["topic"]:
            dt = parse_pubtime(match["time"])
            if dt is not None:
                replay[floor_minute(dt)].add(match["id"])
    return baseline, replay


def window_bounds(args, baseline, replay):
    """Resolve the [since, until] minute-bucket range to report over."""
    all_minutes = set(baseline) | set(replay)
    until = (
        floor_minute(parse_pubtime(args.until))
        if args.until
        else (max(replay) if replay else (max(all_minutes) if all_minutes else None))
    )
    if until is None:
        return None, None
    since = (
        floor_minute(parse_pubtime(args.since))
        if args.since
        else until - timedelta(minutes=args.minutes - 1)
    )
    return since, until


def build_rows(baseline, replay, since, until):
    """One row per minute-with-activity in range: (minute, replay, baseline, diff)."""
    rows = []
    for minute in sorted(set(baseline) | set(replay)):
        if minute < since or minute > until:
            continue
        b = len(baseline.get(minute, ()))
        r = len(replay.get(minute, ()))
        if b == 0 and r == 0:
            continue
        rows.append((minute, r, b, b - r))
    return rows


def window_label(minute: datetime) -> str:
    end = minute + timedelta(minutes=1)
    return f"{minute:%Y-%m-%d %H:%M}–{end:%H:%M}"


def print_table(rows) -> None:
    label_w = max((len(window_label(m)) for m, *_ in rows), default=17)
    header = f"{'Pub-time window (UTC)':<{label_w}}  {'Replay':>8}  {'Baseline':>8}  {'Diff':>7}"
    print(header)
    print("-" * len(header))
    tot_r = tot_b = 0
    for minute, r, b, diff in rows:
        print(f"{window_label(minute):<{label_w}}  {r:>8}  {b:>8}  {diff:>+7}")
        tot_r += r
        tot_b += b
    print("-" * len(header))
    print(f"{'TOTAL':<{label_w}}  {tot_r:>8}  {tot_b:>8}  {tot_b - tot_r:>+7}")


def print_histogram(items, bar_width: int | None, legend: str) -> None:
    """Render an ASCII histogram from ``(label, diff)`` pairs."""
    print()
    print(legend)
    print()
    max_pos = max((diff for _, diff in items if diff > 0), default=0)
    label_w = max((len(label) for label, _ in items), default=17)
    prefix_w = label_w + len(" +0000 | ")
    if bar_width is None:
        term = shutil.get_terminal_size((100, 24)).columns
        bar_width = max(10, term - prefix_w - 1)
    for label, diff in items:
        if diff > 0 and max_pos > 0:
            bar = "#" * max(1, round(diff / max_pos * bar_width))
        elif diff < 0:
            bar = "(replay exceeded baseline)"
        else:
            bar = ""
        print(f"{label:<{label_w}}  {diff:>+5} | {bar}")


# --- summary mode: per-cycle Result lines over the exact tested windows -------
def scan_summary(lines, topic: str):
    """Parse the per-cycle summary lines into per-(window, centre, topic) counts.

    Returns a dict keyed ``(window_start, window_end, centre, topic)`` with
    ``{'baseline','http','mqtt'}`` — the same values SCGRep publishes to
    Prometheus, over the exact tested windows (not clock minutes)."""
    records: dict[tuple, dict[str, int]] = {}
    window: tuple[datetime, datetime] | None = None
    for line in lines:
        period = TESTPERIOD_RE.search(line)
        if period:
            start = parse_pubtime(period["start"])
            end = parse_pubtime(period["end"])
            window = (start, end) if start and end else None
            continue
        result = RESULT_RE.search(line)
        if result and window is not None and topic in result["topic"]:
            key = (window[0], window[1], result["centre"], result["topic"])
            rec = records.setdefault(key, {"baseline": 0, "http": 0, "mqtt": 0})
            rec["baseline"] = int(result["baseline"])
            if result["protocol"] in ("http", "mqtt"):
                rec[result["protocol"]] = int(result["fetched"])
    return records


def window_bounds_summary(args, records):
    """Resolve the [since, until] range (on window start) for summary mode."""
    starts = [key[0] for key in records]
    if not starts:
        return None, None
    until = floor_minute(parse_pubtime(args.until)) if args.until else max(starts)
    if args.since:
        since = parse_pubtime(args.since)
    else:
        since = until - timedelta(minutes=args.minutes)
    return since, until


def build_summary_rows(records, since, until):
    """Aggregate matching series per tested window: (start, end, http, mqtt, baseline).

    Series matching the topic are summed per window (as Grafana sums a multi-topic
    selection); rows with no activity are skipped."""
    agg: dict[tuple, list[int]] = {}
    for (start, end, _centre, _topic), rec in records.items():
        if start < since or start > until:
            continue
        totals = agg.setdefault((start, end), [0, 0, 0])
        totals[0] += rec["baseline"]
        totals[1] += rec["http"]
        totals[2] += rec["mqtt"]
    rows = [
        (start, end, http, mqtt, baseline)
        for (start, end), (baseline, http, mqtt) in agg.items()
        if not (baseline == 0 and http == 0 and mqtt == 0)
    ]
    rows.sort()
    return rows


def summary_label(start: datetime, end: datetime) -> str:
    return f"{start:%Y-%m-%d %H:%M:%S}–{end:%H:%M:%S}"


def print_summary_table(rows) -> None:
    label_w = max((len(summary_label(s, e)) for s, e, *_ in rows), default=23)
    header = (f"{'Pub-time window (UTC)':<{label_w}}  {'Baseline':>8}  {'http':>6}  "
              f"{'mqtt':>6}  {'httpΔ':>6}  {'mqttΔ':>6}")
    print(header)
    print("-" * len(header))
    tot_b = tot_h = tot_m = 0
    for start, end, http, mqtt, baseline in rows:
        print(f"{summary_label(start, end):<{label_w}}  {baseline:>8}  {http:>6}  "
              f"{mqtt:>6}  {baseline - http:>+6}  {baseline - mqtt:>+6}")
        tot_b += baseline
        tot_h += http
        tot_m += mqtt
    print("-" * len(header))
    print(f"{'TOTAL':<{label_w}}  {tot_b:>8}  {tot_h:>6}  {tot_m:>6}  "
          f"{tot_b - tot_h:>+6}  {tot_b - tot_m:>+6}")


def run_summary(args, paths) -> int:
    records = scan_summary(iter_log_lines(paths), args.topic)
    since, until = window_bounds_summary(args, records)
    if since is None:
        print(f"No summary lines found for topic '{args.topic}' in: {', '.join(paths)}",
              file=sys.stderr)
        return 1
    series = sorted({(c, t) for (s, _e, c, t) in records if since <= s <= until})
    rows = build_summary_rows(records, since, until)

    print(f"Topic:  {args.topic}    Source: summary (per tested window)")
    print(f"Window: {since:%Y-%m-%d %H:%M:%S} .. {until:%Y-%m-%d %H:%M:%S} UTC "
          f"(window starts)")
    for centre, topic in series:
        print(f"Series: {centre}  {topic}")
    print()
    if not rows:
        print("No tested windows with activity in the selected range.")
        return 0
    print_summary_table(rows)
    print_histogram(
        [(summary_label(s, e), b - h) for s, e, h, _m, b in rows],
        args.bar_width,
        "Difference histogram (baseline − http numberMatched; "
        "'#' = messages missing from replay):",
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="replay_loss_report.py",
        description=(
            "Scan SCGRep log files and report, minute-by-minute for one topic, "
            "the replay vs baseline message counts and their difference (where a "
            "Global Replay service may be losing messages)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  # last hour for a topic, from the default log file\n"
            "  replay_loss_report.py -t us-noaa-nws\n\n"
            "  # last 30 minutes, synchronous replay only, specific files\n"
            "  replay_loss_report.py -t uk-metoffice -m 30 -s sync logs/scgrep.log\n\n"
            "  # an explicit pub-time window (UTC)\n"
            "  replay_loss_report.py -t int-eumetsat \\\n"
            "      --since 2026-08-16T12:00:00Z --until 2026-08-16T13:00:00Z\n\n"
            "  # per-cycle summary, to line up with the Grafana metrics\n"
            "  replay_loss_report.py -t us-noaa-nws -s summary\n\n"
            "The topic is matched as a substring of the log's topic= field, so "
            "'us-noaa-nws' matches both the concrete baseline topics and the "
            "'.../#' replay wildcard."
        ),
    )
    parser.add_argument(
        "log_files", nargs="*", default=["logs/scgrep.log"],
        help="log file(s) to scan (.log or .gz); default: logs/scgrep.log",
    )
    parser.add_argument(
        "-t", "--topic", required=True,
        help="topic to report on, matched as a substring (e.g. us-noaa-nws)",
    )
    parser.add_argument(
        "-m", "--minutes", type=int, default=60,
        help="length of the reporting window in minutes (default: 60)",
    )
    parser.add_argument(
        "-s", "--source", choices=[*sorted(SOURCES), "summary"], default="both",
        help="what to count. Per-message modes count individual replayed messages "
             "(de-duplicated by id) into clock-minute buckets: 'both' (default), "
             "'sync', or 'async'. 'summary' instead reads the per-cycle summary "
             "lines (baseline / fetched) over the exact tested windows, so counts "
             "match the Prometheus/Grafana metrics.",
    )
    parser.add_argument(
        "--since", metavar="ISO8601",
        help="start of the pub-time window (UTC), overrides --minutes",
    )
    parser.add_argument(
        "--until", metavar="ISO8601",
        help="end of the pub-time window (UTC); default: the most recent "
             "replayed message in the logs",
    )
    parser.add_argument(
        "--bar-width", type=int, default=None, metavar="N",
        help="fixed histogram bar width in characters (default: fit the terminal)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    paths: list[str] = []
    for pattern in args.log_files:
        matches = glob.glob(pattern)
        paths.extend(matches if matches else [pattern])

    if args.source == "summary":
        return run_summary(args, paths)

    baseline, replay = scan(iter_log_lines(paths), args.topic, SOURCES[args.source])
    since, until = window_bounds(args, baseline, replay)
    if since is None:
        print(f"No messages found for topic '{args.topic}' in: {', '.join(paths)}",
              file=sys.stderr)
        return 1

    rows = build_rows(baseline, replay, since, until)
    print(f"Topic:  {args.topic}    Replay source: {args.source}")
    print(f"Window: {since:%Y-%m-%d %H:%M} .. {until + timedelta(minutes=1):%Y-%m-%d %H:%M} UTC "
          f"({args.minutes} min)" if not (args.since or args.until)
          else f"Window: {since:%Y-%m-%d %H:%M} .. {until + timedelta(minutes=1):%Y-%m-%d %H:%M} UTC")
    print()
    if not rows:
        print("No baseline or replay messages in the selected window.")
        return 0
    print_table(rows)
    print_histogram(
        [(window_label(m), diff) for m, _r, _b, diff in rows], args.bar_width,
        "Difference histogram (baseline − replay; '#' = messages missing from replay):",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
