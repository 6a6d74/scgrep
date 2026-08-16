#!/usr/bin/env python3
"""Delete log entries older than a cutoff from a SCGRep log file, in place.

SCGRep log lines begin with a UTC timestamp ``YYYY-MM-DD HH:MM:SS,mmm``. Because
that format is fixed-width and zero-padded, lexicographic order equals
chronological order, so timestamps are compared as strings. Lines without a
leading timestamp (e.g. multi-line tracebacks) inherit the keep/drop decision of
the entry they belong to.

The file is replaced atomically with ``os.replace``; the application's
``WatchedFileHandler`` reopens it on the next write, so purging is safe while the
app is running.

Usage:
    purge_logs.py [LOG_FILE] [--max-age-hours HOURS]

Defaults come from the environment (``PURGE_LOG_FILE`` /
``PURGE_LOG_MAX_AGE_HOURS``) and fall back to
``/var/log/scgrep/scgrep.log`` and ``24``.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import tempfile
from datetime import datetime, timedelta, timezone

_TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")


def purge(log_file: str, max_age_hours: float, now: datetime | None = None) -> tuple[int, int]:
    """Remove entries older than ``max_age_hours`` from ``log_file`` in place.

    Returns ``(kept, removed)`` line counts. A missing file is a no-op.
    """
    if not os.path.isfile(log_file):
        return (0, 0)

    now = now or datetime.now(timezone.utc)
    cutoff = (now - timedelta(hours=max_age_hours)).strftime("%Y-%m-%d %H:%M:%S")

    kept = removed = 0
    keep = True  # leading lines with no timestamp are kept
    directory = os.path.dirname(log_file) or "."
    fd, tmp = tempfile.mkstemp(dir=directory, prefix=".purge-", suffix=".log")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as out, open(
            log_file, "r", encoding="utf-8", errors="replace"
        ) as src:
            for line in src:
                match = _TIMESTAMP.match(line)
                if match:
                    keep = match.group(1) >= cutoff
                if keep:
                    out.write(line)
                    kept += 1
                else:
                    removed += 1
        os.replace(tmp, log_file)  # atomic
    except BaseException:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    return (kept, removed)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "log_file",
        nargs="?",
        default=os.environ.get("PURGE_LOG_FILE", "/var/log/scgrep/scgrep.log"),
        help="path to the log file to purge",
    )
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=float(os.environ.get("PURGE_LOG_MAX_AGE_HOURS", "24")),
        help="delete entries older than this many hours (default 24)",
    )
    args = parser.parse_args(argv)

    if not os.path.isfile(args.log_file):
        print(f"purge-logs: no log file at {args.log_file}; nothing to do", file=sys.stderr)
        return 0

    kept, removed = purge(args.log_file, args.max_age_hours)
    cutoff = (
        datetime.now(timezone.utc) - timedelta(hours=args.max_age_hours)
    ).strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"purge-logs: {args.log_file}: kept {kept}, removed {removed} "
        f"line(s) older than {cutoff} UTC",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
