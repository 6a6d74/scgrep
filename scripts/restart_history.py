#!/usr/bin/env python3
"""Show when SCGRep restarted, and the broker connections that followed.

Useful because a restart explains metric artefacts that otherwise look like
faults — most obviously the baseline reading **zero** for roughly
``TIME_LAG + TEST_INTERVAL`` afterwards, while Redis refills from the live broker
feed (see the README's Troubleshooting section).

Reads two sources and merges them, because neither is complete on its own:

* ``docker logs <container>`` — only ever covers the **current** container, so it
  is lost on every ``docker compose up``/recreate;
* the log file (``LOG_FILE``, default ``logs/scgrep.log``) — survives restarts,
  but is trimmed by the hourly purge.

Duplicate lines (present in both) are collapsed. Standard library only; run with
``-h`` / ``--help`` for usage.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import date, datetime, timedelta

START_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}),\d+ .*"
    r"Starting SCGRep sensor centre (\S+) \(subscriber-id=(\S+?)\)"
)
CONNECT_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}),\d+ .*"
    r"Connected to (Global Broker|Replay broker|Redis)\s*(.*)$"
)
DISCONNECT_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2}),\d+ .*Disconnected from (\S+?);"
)


def read_sources(container: str, log_file: str, use_docker: bool) -> list[str]:
    """Merged, de-duplicated lines from `docker logs` and the log file."""
    lines: list[str] = []
    if use_docker:
        try:
            proc = subprocess.run(
                ["docker", "logs", container],
                capture_output=True, text=True, timeout=120,
            )
            lines += (proc.stdout + proc.stderr).splitlines()
        except (OSError, subprocess.SubprocessError) as exc:
            print(f"warning: could not read `docker logs {container}`: {exc}",
                  file=sys.stderr)
    try:
        with open(log_file, errors="replace") as fh:
            lines += fh.read().splitlines()
    except FileNotFoundError:
        print(f"warning: log file not found: {log_file}", file=sys.stderr)
    # The same line can appear in both sources; the timestamp prefix makes a
    # sorted set chronological.
    return sorted(set(lines))


def collect(lines, wanted_dates):
    """Return [(kind, date, time, detail)] for restart/connection events."""
    events = []
    for line in lines:
        for kind, rx in (("start", START_RE), ("connect", CONNECT_RE),
                         ("disconnect", DISCONNECT_RE)):
            m = rx.match(line)
            if not m:
                continue
            d, t = m.group(1), m.group(2)
            if wanted_dates and d not in wanted_dates:
                break
            if kind == "start":
                detail = f"{m.group(3)}  subscriber-id={m.group(4)}"
            elif kind == "connect":
                detail = f"{m.group(3)} {m.group(4)}".strip()
            else:
                detail = m.group(3)
            events.append((kind, d, t, detail))
            break
    events.sort(key=lambda e: (e[1], e[2]))
    return events


def seconds_between(d1, t1, d2, t2) -> float:
    fmt = "%Y-%m-%d %H:%M:%S"
    return (datetime.strptime(f"{d2} {t2}", fmt)
            - datetime.strptime(f"{d1} {t1}", fmt)).total_seconds()


def hms_delta(d1, t1, d2, t2) -> str:
    fmt = "%Y-%m-%d %H:%M:%S"
    delta = datetime.strptime(f"{d2} {t2}", fmt) - datetime.strptime(f"{d1} {t1}", fmt)
    s = int(delta.total_seconds())
    return f"{s // 3600}h{s % 3600 // 60:02d}m" if s >= 3600 else f"{s // 60}m{s % 60:02d}s"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="restart_history.py",
        description="Show when SCGRep restarted, with the broker connections "
                    "that followed each restart.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  restart_history.py                 # today\n"
            "  restart_history.py --days 3        # today and the two days before\n"
            "  restart_history.py --date 2026-08-18\n"
            "  restart_history.py --all --disconnects\n\n"
            "A restart explains a baseline of zero for roughly "
            "TIME_LAG + TEST_INTERVAL afterwards:\nSCGRep builds the baseline from "
            "the live broker feed, so it cannot count messages\npublished before it "
            "connected."
        ),
    )
    p.add_argument("--container", default="scgrep", help="container name (default: scgrep)")
    p.add_argument("--log-file", default="logs/scgrep.log",
                   help="log file to merge in (default: logs/scgrep.log)")
    p.add_argument("--no-docker", action="store_true",
                   help="read only the log file, not `docker logs`")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--date", metavar="YYYY-MM-DD", help="a single date (default: today)")
    g.add_argument("--days", type=int, metavar="N", help="the last N days, including today")
    g.add_argument("--all", action="store_true", help="every date present in the logs")
    p.add_argument("--disconnects", action="store_true",
                   help="also show broker disconnects (reveals connection flapping)")
    p.add_argument("--connections", action="store_true",
                   help="list every connection event, not just those at start-up "
                        "(later reconnects are summarised by default)")
    p.add_argument("--settle", type=int, default=60, metavar="SEC",
                   help="seconds after a restart within which a connection counts "
                        "as part of start-up (default: 60)")
    args = p.parse_args(argv)

    if args.all:
        wanted = None
    elif args.date:
        wanted = {args.date}
    elif args.days:
        today = date.today()
        wanted = {str(today - timedelta(days=i)) for i in range(args.days)}
    else:
        wanted = {str(date.today())}

    lines = read_sources(args.container, args.log_file, not args.no_docker)
    events = collect(lines, wanted)
    if not args.disconnects:
        events = [e for e in events if e[0] != "disconnect"]

    scope = "all dates" if wanted is None else ", ".join(sorted(wanted))
    print(f"SCGRep restart history — {scope}")
    print(f"sources: {'docker logs ' + args.container + ' + ' if not args.no_docker else ''}"
          f"{args.log_file}\n")
    if not events:
        print("No restart or connection events found.")
        return 1

    starts = [e for e in events if e[0] == "start"]
    prev = None            # previous restart (date, time)
    later = 0              # reconnects since the last restart, not at start-up
    orphans = 0            # connection events before any restart in scope

    def flush_later():
        nonlocal later
        if later and not args.connections:
            print(f"    └─ …plus {later} later reconnection(s) "
                  f"— pass --connections to list them")
        later = 0

    for kind, d, t, detail in events:
        if kind == "start":
            flush_later()
            gap = f"   (+{hms_delta(*prev, d, t)} since previous restart)" if prev else ""
            print(f"\n● RESTART  {d} {t}  {detail}{gap}")
            prev = (d, t)
            continue
        label = "connected   " if kind == "connect" else "DISCONNECT  "
        if prev is None:
            # Belongs to a restart that predates the window being shown.
            orphans += 1
            if args.connections:
                print(f"    ·  {label} {d} {t}  {detail}")
            continue
        at_startup = seconds_between(*prev, d, t) <= args.settle
        if at_startup or args.connections:
            print(f"    ├─ {label} {t}  {detail}")
        else:
            later += 1
    flush_later()
    if orphans and not args.connections:
        print(f"\n({orphans} connection event(s) before the first restart shown — "
              f"reconnects from an earlier run; use --connections to list them)")

    print(f"\n{len(starts)} restart(s).", end="")
    if starts:
        _, d, t, _ = starts[-1]
        print(f" Most recent: {d} {t}"
              f" ({hms_delta(d, t, str(date.today()), datetime.now().strftime('%H:%M:%S'))} ago).")
    else:
        print(" (Connection events shown may belong to a restart outside the window.)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
