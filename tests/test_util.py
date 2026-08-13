from scgrep.util import (
    broker_authority,
    epoch_to_iso,
    parse_time_to_epoch,
    topic_matches,
)


def test_parse_time_with_z():
    assert parse_time_to_epoch("1970-01-01T00:00:00Z") == 0.0


def test_parse_time_with_offset():
    # 01:00:00+01:00 == 00:00:00Z
    assert parse_time_to_epoch("1970-01-01T01:00:00+01:00") == 0.0


def test_parse_time_naive_assumed_utc():
    assert parse_time_to_epoch("1970-01-01T00:00:00") == 0.0


def test_roundtrip_iso():
    assert epoch_to_iso(0.0) == "1970-01-01T00:00:00Z"
    assert epoch_to_iso(parse_time_to_epoch("2026-03-19T12:25:00Z")) == (
        "2026-03-19T12:25:00Z"
    )


def test_topic_matches_wildcards():
    assert topic_matches("cache/a/wis2/ca-eccc-msc/data/#", "cache/a/wis2/ca-eccc-msc/data/core/x")
    assert topic_matches("monitor/a/wis2/+", "monitor/a/wis2/ca-eccc-msc")
    assert not topic_matches("cache/a/wis2/other/#", "cache/a/wis2/ca-eccc-msc/data/x")


def test_broker_authority_ignores_credentials_and_defaults_port():
    assert broker_authority("mqtts://everyone:everyone@globalbroker.meteo.fr:8883") == (
        "globalbroker.meteo.fr:8883"
    )
    assert broker_authority("mqtts://globalbroker.meteo.fr") == (
        "globalbroker.meteo.fr:8883"
    )
