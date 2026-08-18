from scgrep.util import (
    broker_authority,
    epoch_to_iso,
    parse_time_to_epoch,
    topic_matches,
    topic_to_collection,
    topic_to_query,
)


def test_topic_to_collection_routes_by_topic_tree():
    # Data notifications (origin/ and cache/ trees).
    assert topic_to_collection("cache/a/wis2/uk-metoffice/#") == "wis2-notification-messages"
    assert topic_to_collection("origin/a/wis2/uk-metoffice/data/core") == (
        "wis2-notification-messages"
    )
    # WIS2 Monitoring Event Messages.
    assert topic_to_collection("monitor/a/wis2/ca-eccc-msc") == (
        "wis2-monitoring-event-messages"
    )
    assert topic_to_collection("replay/a/wis2/ca-eccc-msc/uuid/#") == (
        "wis2-monitoring-event-messages"
    )
    # A centre whose name merely starts with "monitor" is not the monitor tree.
    assert topic_to_collection("cache/a/wis2/monitor-centre/x") == (
        "wis2-notification-messages"
    )


def test_topic_to_query_strips_mqtt_wildcard_and_trailing_slash():
    # Global Replay matches whole topic levels as a prefix: a trailing '/#' or
    # '/' matches nothing, so both must be stripped before querying.
    assert topic_to_query("cache/a/wis2/uk-metoffice/#") == "cache/a/wis2/uk-metoffice"
    assert topic_to_query("cache/a/wis2/uk-metoffice/") == "cache/a/wis2/uk-metoffice"
    assert topic_to_query("cache/a/wis2/uk-metoffice//") == "cache/a/wis2/uk-metoffice"
    # Already in query form, or a deeper prefix: unchanged.
    assert topic_to_query("cache/a/wis2/uk-metoffice") == "cache/a/wis2/uk-metoffice"
    assert topic_to_query("cache/a/wis2/uk-metoffice/data/core") == (
        "cache/a/wis2/uk-metoffice/data/core"
    )
    # A '#' inside a level is not an MQTT wildcard and must not be mangled.
    assert topic_to_query("cache/a/wis2/x#y") == "cache/a/wis2/x#y"
    # Bare '#' has no Global Replay equivalent.
    assert topic_to_query("#") == ""
    assert topic_to_query("  cache/a/wis2/x/#  ") == "cache/a/wis2/x"


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
