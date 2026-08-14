import threading
import time
import types

import scgrep.main as main


def test_scheduler_runs_once_per_interval(monkeypatch):
    """Cycles must start every TEST_INTERVAL, not every ~2x interval.

    Regression test for a double-wait bug where the loop slept a full interval
    at the top *and* the remainder at the bottom of each iteration.
    """
    timestamps: list[float] = []
    monkeypatch.setattr(
        main, "run_cycle", lambda *a, **k: timestamps.append(time.monotonic())
    )

    interval = 0.2
    cfg = types.SimpleNamespace(test_interval=interval)
    scheduler = main.Scheduler(cfg, store=None, registry=None, metrics=None)

    thread = threading.Thread(target=scheduler.run, daemon=True)
    start = time.monotonic()
    thread.start()
    # ~3.7 intervals: expect cycles at ~1x, 2x, 3x interval => 3 runs.
    time.sleep(interval * 3.7)
    scheduler.stop()
    thread.join(timeout=2)

    # Fixed cadence yields ~3 runs in this window; the buggy double-wait yields 1-2.
    assert len(timestamps) >= 3
    # First cycle should be ~one interval after start, not immediate.
    assert timestamps[0] - start >= interval * 0.8
    # Consecutive cycles should be spaced ~one interval apart (not ~two).
    gaps = [b - a for a, b in zip(timestamps, timestamps[1:])]
    assert all(gap < interval * 1.6 for gap in gaps), gaps


def test_scheduler_stops_promptly(monkeypatch):
    monkeypatch.setattr(main, "run_cycle", lambda *a, **k: None)
    cfg = types.SimpleNamespace(test_interval=10)
    scheduler = main.Scheduler(cfg, store=None, registry=None, metrics=None)
    thread = threading.Thread(target=scheduler.run, daemon=True)
    thread.start()
    scheduler.stop()
    thread.join(timeout=2)
    assert not thread.is_alive()
