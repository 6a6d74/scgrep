import importlib.util
import pathlib
from datetime import datetime, timezone

# Load the standalone script (it lives in scripts/, not the package).
_PATH = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "purge_logs.py"
_spec = importlib.util.spec_from_file_location("purge_logs", _PATH)
purge_logs = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(purge_logs)


NOW = datetime(2026, 8, 16, 11, 0, 0, tzinfo=timezone.utc)


def test_purge_removes_old_keeps_recent(tmp_path):
    log = tmp_path / "scgrep.log"
    log.write_text(
        "2026-08-16 08:00:00,000 INFO [x] old line\n"
        "  traceback continuation of old\n"
        "2026-08-16 10:30:00,000 INFO [x] recent line\n"
        "  traceback continuation of recent\n"
    )
    # cutoff = 11:00 - 2h = 09:00 UTC
    kept, removed = purge_logs.purge(str(log), max_age_hours=2, now=NOW)
    text = log.read_text()
    assert "old line" not in text
    assert "continuation of old" not in text
    assert "recent line" in text
    assert "continuation of recent" in text
    assert (kept, removed) == (2, 2)


def test_purge_keeps_everything_when_all_recent(tmp_path):
    log = tmp_path / "scgrep.log"
    log.write_text(
        "2026-08-16 10:59:00,000 INFO [x] a\n"
        "2026-08-16 10:59:30,000 INFO [x] b\n"
    )
    kept, removed = purge_logs.purge(str(log), max_age_hours=24, now=NOW)
    assert (kept, removed) == (2, 0)
    assert log.read_text().count("INFO") == 2


def test_purge_missing_file_is_noop(tmp_path):
    assert purge_logs.purge(str(tmp_path / "nope.log"), max_age_hours=24, now=NOW) == (0, 0)


def test_purge_boundary_is_inclusive(tmp_path):
    log = tmp_path / "scgrep.log"
    # Exactly at the cutoff (09:00) is kept; one second before is removed.
    log.write_text(
        "2026-08-16 08:59:59,000 INFO [x] just too old\n"
        "2026-08-16 09:00:00,000 INFO [x] exactly at cutoff\n"
    )
    kept, removed = purge_logs.purge(str(log), max_age_hours=2, now=NOW)
    text = log.read_text()
    assert "just too old" not in text
    assert "exactly at cutoff" in text
    assert (kept, removed) == (1, 1)
