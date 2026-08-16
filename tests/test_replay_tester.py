import logging
import threading
import time

import fakeredis
import requests
import responses

from scgrep.redis_store import RedisStore
from scgrep.replay_registry import ReplayRegistry
from scgrep.replay_tester import (
    async_fetch,
    sync_fetch,
    _validate_subscriptions,
)

REPLAY_URL = "https://replay.example.org"
ITEMS_URL = f"{REPLAY_URL}/collections/wis2-notification-messages/items"
EXEC_URL = f"{REPLAY_URL}/processes/wis2-grep-subscriber/execution"
TOPIC = "monitor/a/wis2/ca-eccc-msc"
CENTRE = "ca-eccc-msc-global-replay"


# --------------------------------------------------------------------------
# Synchronous (OGC API - Features) fetch
# --------------------------------------------------------------------------

def _store():
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisStore("redis:6379", 660, [TOPIC], client=client)


def _feature(msg_id, pubtime="2026-08-16T09:00:00Z"):
    return {"type": "Feature", "id": msg_id, "properties": {"pubtime": pubtime}}


def _collection(number_matched, features, next_href=None):
    doc = {
        "type": "FeatureCollection",
        "numberMatched": number_matched,
        "features": features,
        "links": [],
    }
    if next_href:
        doc["links"].append({"rel": "next", "type": "application/geo+json", "href": next_href})
    return doc


@responses.activate
def test_sync_fetch_counts_stores_and_matches():
    responses.add(
        responses.GET, ITEMS_URL,
        json=_collection(2, [_feature("m1"), _feature("m2")]), status=200,
    )
    store = _store()
    result = sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 5.0, store)
    assert result.protocol == "http"
    assert result.aborted is False
    assert result.invalid_format is False
    assert result.invalid_number_matched is False
    assert result.messages_fetched == 2  # numberMatched
    assert store.count_sync_messages(CENTRE, TOPIC, 0, 9_999_999_999) == 2


@responses.activate
def test_sync_fetch_number_matched_mismatch(caplog):
    # numberMatched says 5 but only one Feature is returned (no next page).
    responses.add(responses.GET, ITEMS_URL, json=_collection(5, [_feature("m1")]), status=200)
    with caplog.at_level(logging.ERROR, logger="scgrep.replay_tester"):
        result = sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 5.0, _store())
    assert result.messages_fetched == 5  # numberMatched is unchanged
    assert result.invalid_number_matched is True
    assert "numberMatched mismatch" in caplog.text


@responses.activate
def test_sync_fetch_pages_through_next_links():
    next_url = f"{REPLAY_URL}/items-page-2"
    responses.add(
        responses.GET, ITEMS_URL,
        json=_collection(3, [_feature("a"), _feature("b")], next_href=next_url), status=200,
    )
    responses.add(responses.GET, next_url, json=_collection(3, [_feature("c")]), status=200)
    store = _store()
    result = sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 5.0, store)
    assert result.messages_fetched == 3
    assert result.invalid_number_matched is False  # 3 features seen == numberMatched
    assert store.count_sync_messages(CENTRE, TOPIC, 0, 9_999_999_999) == 3


@responses.activate
def test_sync_fetch_missing_number_matched():
    responses.add(responses.GET, ITEMS_URL, json={"features": []}, status=200)
    result = sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 5.0, _store())
    assert result.invalid_format is True
    assert result.messages_fetched == 0
    assert result.aborted is False


@responses.activate
def test_sync_fetch_non_json():
    responses.add(responses.GET, ITEMS_URL, body="<html>oops</html>", status=200)
    result = sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 5.0, _store())
    assert result.invalid_format is True
    assert result.messages_fetched == 0


@responses.activate
def test_sync_fetch_timeout_aborts():
    responses.add(responses.GET, ITEMS_URL, body=requests.exceptions.ReadTimeout())
    result = sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 2.0, _store())
    assert result.aborted is True
    assert result.invalid_format is True
    assert result.messages_fetched == 0
    assert result.fetch_delay_ms == 2.0 * 1000


@responses.activate
def test_sync_fetch_emits_request_response_message_and_number_matched_logs(caplog):
    responses.add(
        responses.GET, ITEMS_URL,
        json=_collection(2, [_feature("m1"), _feature("m2")]), status=200,
    )
    with caplog.at_level(logging.INFO, logger="scgrep.replay_tester"):
        sync_fetch(REPLAY_URL, CENTRE, TOPIC, "s", "e", 5.0, _store(), max_chars=40)
    text = caplog.text
    assert "type=synchronous" in text and CENTRE in text and TOPIC in text
    assert "request=GET" in text and "datetime=s/e" in text
    assert "Global Replay response" in text
    assert "truncated" in text  # body exceeded max_chars=40
    assert f"Replay message (synchronous): centre_id={CENTRE} topic={TOPIC} id=m1" in text
    assert "numberMatched=2" in text


# --------------------------------------------------------------------------
# Subscription-metadata validation
# --------------------------------------------------------------------------

def _valid_metadata(channel, hrefs):
    return {
        "subscriptions": [
            {
                "rel": "items",
                "type": "application/json",
                "href": href,
                "title": "Broker",
                "channel": channel,
            }
            for href in hrefs
        ]
    }


PREFIX = "replay/a/wis2/c/uuid/"


def test_validate_subscriptions_wildcard_channel_ok():
    # The deployed service returns one subscriber-scoped wildcard channel.
    meta = _valid_metadata(PREFIX + "#", ["mqtts://everyone:everyone@globalbroker.meteo.fr:8883"])
    assert _validate_subscriptions(meta, PREFIX, ["globalbroker.meteo.fr:8883"])


def test_validate_subscriptions_per_topic_channel_ok():
    # A per-topic channel within the namespace is also accepted.
    meta = _valid_metadata(PREFIX + TOPIC, ["mqtts://globalbroker.meteo.fr:8883"])
    assert _validate_subscriptions(meta, PREFIX, ["globalbroker.meteo.fr:8883"])


def test_validate_subscriptions_channel_outside_namespace():
    meta = _valid_metadata("replay/a/wis2/other/uuid/#", ["mqtts://globalbroker.meteo.fr:8883"])
    assert not _validate_subscriptions(meta, PREFIX, ["globalbroker.meteo.fr:8883"])


def test_validate_subscriptions_missing_broker():
    meta = _valid_metadata(PREFIX + "#", ["mqtts://other.broker:8883"])
    assert not _validate_subscriptions(meta, PREFIX, ["globalbroker.meteo.fr:8883"])


def test_validate_subscriptions_empty():
    assert not _validate_subscriptions({"subscriptions": []}, PREFIX, [])
    assert not _validate_subscriptions({}, PREFIX, [])


# --------------------------------------------------------------------------
# Asynchronous (OGC API - Processes + MQTT) fetch
# --------------------------------------------------------------------------

@responses.activate
def test_async_fetch_receives_messages():
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    wildcard = f"replay/a/wis2/{centre_id}/{subscriber_id}/#"
    channel = f"replay/a/wis2/{centre_id}/{subscriber_id}/{TOPIC}"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata(wildcard, ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()

    # Simulate replay messages arriving on the concrete per-topic replay topic.
    def deliver():
        time.sleep(0.1)
        registry.handle_replay(channel)
        registry.handle_replay(channel)

    threading.Thread(target=deliver, daemon=True).start()

    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "2026-03-19T12:20:00Z", "2026-03-19T12:25:00Z",
        deadline_s=0.5, registry=registry, baseline=5, poll_interval=0.02,
    )
    assert result.protocol == "mqtt"
    assert result.aborted is False
    assert result.invalid_format is False
    assert result.messages_fetched == 2
    assert result.fetch_delay_ms > 0


@responses.activate
def test_async_fetch_aborts_without_messages():
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    wildcard = f"replay/a/wis2/{centre_id}/{subscriber_id}/#"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata(wildcard, ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()
    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=0.2, registry=registry, baseline=5, poll_interval=0.02,
    )
    assert result.aborted is True
    assert result.messages_fetched == 0
    assert result.fetch_delay_ms == 0.2 * 1000
    # Metadata was valid, so invalid_format stays False.
    assert result.invalid_format is False


@responses.activate
def test_async_fetch_invalid_metadata_flag():
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    channel = f"replay/a/wis2/{centre_id}/{subscriber_id}/{TOPIC}"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata("replay/a/wis2/c/uuid/wrong", ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()

    def deliver():
        time.sleep(0.05)
        registry.handle_replay(channel)

    threading.Thread(target=deliver, daemon=True).start()

    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=0.3, registry=registry, baseline=5, poll_interval=0.02,
    )
    assert result.invalid_format is True
    assert result.messages_fetched == 1


@responses.activate
def test_async_fetch_zero_baseline_uses_http_delay():
    # With a zero baseline no replay messages are expected: the fetch must not
    # abort and must report the HTTP response delay, returning promptly.
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    wildcard = f"replay/a/wis2/{centre_id}/{subscriber_id}/#"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata(wildcard, ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()
    t0 = time.monotonic()
    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=5.0, registry=registry, baseline=0, poll_interval=0.02,
    )
    elapsed = time.monotonic() - t0
    assert result.protocol == "mqtt"
    assert result.aborted is False
    assert result.messages_fetched == 0
    assert result.invalid_format is False
    # HTTP response delay, not the 95% abort value.
    assert 0 <= result.fetch_delay_ms < 5.0 * 1000
    # Did not wait for the deadline.
    assert elapsed < 1.0


@responses.activate
def test_async_fetch_zero_baseline_still_validates_metadata():
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata("replay/a/wis2/c/uuid/wrong", ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()
    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=5.0, registry=registry, baseline=0, poll_interval=0.02,
    )
    assert result.aborted is False
    assert result.messages_fetched == 0
    assert result.invalid_format is True  # channel outside the subscriber namespace
    assert result.fetch_delay_ms >= 0


@responses.activate
def test_async_fetch_number_matched_zero_does_not_abort():
    # numberMatched == 0 (a genuine replay gap) even though the baseline is
    # non-zero: no replay messages are expected, so the fetch must NOT abort — it
    # reports the HTTP response delay and returns promptly.
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    wildcard = f"replay/a/wis2/{centre_id}/{subscriber_id}/#"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata(wildcard, ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()
    t0 = time.monotonic()
    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=5.0, registry=registry, baseline=204,
        number_matched_provider=lambda: 0, poll_interval=0.02,
    )
    elapsed = time.monotonic() - t0
    assert result.aborted is False
    assert result.messages_fetched == 0
    assert result.invalid_format is False
    assert 0 <= result.fetch_delay_ms < 5.0 * 1000  # HTTP delay, not the abort value
    assert elapsed < 1.0  # did not wait for the deadline


@responses.activate
def test_async_fetch_number_matched_positive_overrides_zero_baseline():
    # numberMatched > 0 even though the baseline is zero: messages ARE expected,
    # so the fetch waits for them and counts (does not short-circuit).
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    wildcard = f"replay/a/wis2/{centre_id}/{subscriber_id}/#"
    channel = f"replay/a/wis2/{centre_id}/{subscriber_id}/{TOPIC}"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata(wildcard, ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()

    def deliver():
        time.sleep(0.05)
        registry.handle_replay(channel)

    threading.Thread(target=deliver, daemon=True).start()

    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=0.4, registry=registry, baseline=0,
        number_matched_provider=lambda: 3, poll_interval=0.02,
    )
    assert result.aborted is False
    assert result.messages_fetched == 1
    assert result.fetch_delay_ms > 0


@responses.activate
def test_async_fetch_number_matched_unavailable_falls_back_to_baseline():
    # numberMatched unavailable (provider returns None, e.g. the synchronous
    # fetch failed): fall back to the baseline. Non-zero baseline + no messages
    # -> abort.
    centre_id = "ca-eccc-msc-global-replay"
    subscriber_id = "uuid-1234"
    wildcard = f"replay/a/wis2/{centre_id}/{subscriber_id}/#"
    responses.add(
        responses.POST, EXEC_URL,
        json=_valid_metadata(wildcard, ["mqtts://globalbroker.meteo.fr:8883"]),
        status=200,
    )
    registry = ReplayRegistry()
    result = async_fetch(
        REPLAY_URL, centre_id, TOPIC, subscriber_id,
        ["globalbroker.meteo.fr:8883"],
        "s", "e", deadline_s=0.2, registry=registry, baseline=5,
        number_matched_provider=lambda: None, poll_interval=0.02,
    )
    assert result.aborted is True
    assert result.messages_fetched == 0
