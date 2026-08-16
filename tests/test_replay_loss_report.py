import importlib.util
import pathlib
from datetime import datetime, timezone

# Load the standalone script (it lives in scripts/, not the package).
_PATH = pathlib.Path(__file__).resolve().parent.parent / "scripts" / "replay_loss_report.py"
_spec = importlib.util.spec_from_file_location("replay_loss_report", _PATH)
rlr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rlr)


def _baseline(topic, msg_id, pubtime):
    return (f"2026-08-16 12:00:00,000 INFO [scgrep.mqtt_client] Global Broker "
            f"message: topic={topic} id={msg_id} time={pubtime}\n")


def _replay(kind, topic, msg_id, pubtime):
    return (f"2026-08-16 12:05:00,000 INFO [scgrep.replay_tester] Replay message "
            f"({kind}): centre_id=ca-eccc-msc-global-replay topic={topic} "
            f"id={msg_id} time={pubtime}\n")


def test_parse_pubtime_variants():
    assert rlr.parse_pubtime("2026-08-16T12:08:15Z") == datetime(
        2026, 8, 16, 12, 8, 15, tzinfo=timezone.utc)
    # fractional seconds and no timezone still parse to UTC
    assert rlr.parse_pubtime("2026-08-16T12:08:15.37Z").minute == 8
    assert rlr.parse_pubtime("2026-08-16T12:08:15").tzinfo == timezone.utc
    assert rlr.parse_pubtime("not-a-time") is None


def test_scan_counts_and_dedups_per_minute():
    topic = "cache/a/wis2/us-noaa-nws"
    lines = [
        _baseline(topic + "/data/x", "b1", "2026-08-16T12:08:10Z"),
        _baseline(topic + "/data/y", "b2", "2026-08-16T12:08:59Z"),
        _baseline(topic + "/data/y", "b2", "2026-08-16T12:08:59Z"),  # dup id -> once
        _baseline(topic + "/data/y", "b3", "2026-08-16T12:09:01Z"),  # next minute
        # replay: wildcard topic form, one shared across sync+async (dedup by id)
        _replay("synchronous", topic + "/#", "b1", "2026-08-16T12:08:10Z"),
        _replay("asynchronous", topic + "/data/x", "b1", "2026-08-16T12:08:10Z"),
    ]
    baseline, replay = rlr.scan(lines, "us-noaa-nws", rlr.SOURCES["both"])
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    m09 = datetime(2026, 8, 16, 12, 9, tzinfo=timezone.utc)
    assert len(baseline[m08]) == 2  # b1, b2 (dup collapsed)
    assert len(baseline[m09]) == 1  # b3
    assert len(replay[m08]) == 1    # b1 counted once across sync+async


def test_source_filtering():
    topic = "cache/a/wis2/uk-metoffice"
    lines = [
        _replay("synchronous", topic + "/#", "s1", "2026-08-16T12:08:00Z"),
        _replay("asynchronous", topic + "/data/z", "a1", "2026-08-16T12:08:00Z"),
    ]
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    _, sync_only = rlr.scan(lines, "uk-metoffice", rlr.SOURCES["sync"])
    _, async_only = rlr.scan(lines, "uk-metoffice", rlr.SOURCES["async"])
    assert sync_only[m08] == {"s1"}
    assert async_only[m08] == {"a1"}


def test_topic_substring_isolation():
    lines = [
        _baseline("cache/a/wis2/us-noaa-nws/data/x", "b1", "2026-08-16T12:08:00Z"),
        _baseline("cache/a/wis2/uk-metoffice/data/x", "b2", "2026-08-16T12:08:00Z"),
    ]
    baseline, _ = rlr.scan(lines, "us-noaa-nws", rlr.SOURCES["both"])
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    assert baseline[m08] == {"b1"}  # uk-metoffice excluded


def test_build_rows_diff_and_skips_empty():
    m08 = datetime(2026, 8, 16, 12, 8, tzinfo=timezone.utc)
    m09 = datetime(2026, 8, 16, 12, 9, tzinfo=timezone.utc)
    m10 = datetime(2026, 8, 16, 12, 10, tzinfo=timezone.utc)
    baseline = {m08: {"a", "b", "c"}, m09: {"d"}}
    replay = {m08: {"a"}, m10: {"e", "f"}}
    rows = rlr.build_rows(baseline, replay, m08, m10)
    # (minute, replay, baseline, diff); m09 has baseline 1/replay 0 -> +1
    assert rows == [
        (m08, 1, 3, 2),
        (m09, 0, 1, 1),
        (m10, 2, 0, -2),
    ]


def test_window_bounds_default_anchors_to_latest_replay():
    class Args:
        since = None
        until = None
        minutes = 3
    m05 = datetime(2026, 8, 16, 12, 5, tzinfo=timezone.utc)
    m20 = datetime(2026, 8, 16, 12, 20, tzinfo=timezone.utc)
    m18 = datetime(2026, 8, 16, 12, 18, tzinfo=timezone.utc)
    baseline = {m20: {"late-baseline"}}
    replay = {m05: {"r1"}, m18: {"r2"}}
    since, until = rlr.window_bounds(Args(), baseline, replay)
    assert until == m18  # latest replay, not the later baseline
    assert since == datetime(2026, 8, 16, 12, 16, tzinfo=timezone.utc)  # 3-min window


def _period(start, end):
    return (f"2026-08-16 12:15:00,000 INFO [scgrep.test_cycle] Test period begins: "
            f"window {start} .. {end}\n")


def _result(centre, topic, protocol, baseline, fetched):
    return (f"2026-08-16 12:15:00,000 INFO [scgrep.test_cycle] Result: "
            f"centre_id={centre} topic={topic} protocol={protocol} "
            f"baseline={baseline} fetched={fetched} delay_ms=100 aborted=0 "
            f"invalid_format=0 invalid_numberMatched=0\n")


def test_scan_summary_pairs_window_with_results():
    topic = "cache/a/wis2/us-noaa-nws/#"
    lines = [
        _period("2026-08-16T12:08:52Z", "2026-08-16T12:09:52Z"),
        _result("ca-eccc", topic, "http", 420, 251),
        _result("ca-eccc", topic, "mqtt", 420, 240),
        _result("ca-eccc", "cache/a/wis2/uk-metoffice/#", "http", 6, 6),  # other topic
    ]
    records = rlr.scan_summary(lines, "us-noaa-nws")
    start = datetime(2026, 8, 16, 12, 8, 52, tzinfo=timezone.utc)
    end = datetime(2026, 8, 16, 12, 9, 52, tzinfo=timezone.utc)
    assert records[(start, end, "ca-eccc", topic)] == {
        "baseline": 420, "http": 251, "mqtt": 240}
    assert len(records) == 1  # uk-metoffice excluded by the topic filter


def test_build_summary_rows_aggregates_and_skips_empty():
    s1 = datetime(2026, 8, 16, 12, 8, 52, tzinfo=timezone.utc)
    e1 = datetime(2026, 8, 16, 12, 9, 52, tzinfo=timezone.utc)
    s2 = datetime(2026, 8, 16, 12, 9, 52, tzinfo=timezone.utc)
    e2 = datetime(2026, 8, 16, 12, 10, 52, tzinfo=timezone.utc)
    records = {
        # two matching series in the same window -> summed
        (s1, e1, "c", "a"): {"baseline": 400, "http": 251, "mqtt": 240},
        (s1, e1, "c", "b"): {"baseline": 20, "http": 0, "mqtt": 0},
        (s2, e2, "c", "a"): {"baseline": 0, "http": 0, "mqtt": 0},  # empty -> skipped
    }
    rows = rlr.build_summary_rows(records, s1, e2)
    assert rows == [(s1, e1, 251, 240, 420)]  # (start, end, http, mqtt, baseline)


def test_run_summary_end_to_end(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/us-noaa-nws/#"
    log.write_text(
        _period("2026-08-16T12:08:52Z", "2026-08-16T12:09:52Z")
        + _result("ca-eccc", topic, "http", 420, 0)   # a genuine gap
        + _result("ca-eccc", topic, "mqtt", 420, 0)
    )
    rc = rlr.main(["-t", "us-noaa-nws", "-s", "summary",
                   "--since", "2026-08-16T12:00:00Z", "--until", "2026-08-16T12:30:00Z",
                   str(log)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Source: summary" in out
    assert "12:08:52–12:09:52" in out
    assert "+420" in out  # baseline 420 - http 0
    assert "http numberMatched" in out  # histogram legend


def test_main_end_to_end(tmp_path, capsys):
    log = tmp_path / "scgrep.log"
    topic = "cache/a/wis2/us-noaa-nws"
    log.write_text(
        _baseline(topic + "/data/x", "b1", "2026-08-16T12:08:10Z")
        + _baseline(topic + "/data/x", "b2", "2026-08-16T12:08:20Z")
        + _replay("synchronous", topic + "/#", "b1", "2026-08-16T12:08:10Z")
    )
    rc = rlr.main(["-t", "us-noaa-nws", "--since", "2026-08-16T12:08:00Z",
                   "--until", "2026-08-16T12:08:30Z", str(log)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "12:08–12:09" in out
    assert "Difference histogram" in out
    assert "+1" in out  # baseline 2 - replay 1
