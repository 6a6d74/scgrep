import threading
import time

import requests
import responses

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


# --------------------------------------------------------------------------
# Synchronous (OGC API - Features) fetch
# --------------------------------------------------------------------------

@responses.activate
def test_sync_fetch_success():
    responses.add(responses.GET, ITEMS_URL, json={"numberMatched": 42}, status=200)
    result = sync_fetch(REPLAY_URL, TOPIC, "2026-03-19T12:20:00Z", "2026-03-19T12:25:00Z", 5.0)
    assert result.protocol == "http"
    assert result.aborted is False
    assert result.invalid_format is False
    assert result.messages_fetched == 42
    assert result.fetch_delay_ms >= 0


@responses.activate
def test_sync_fetch_missing_number_matched():
    responses.add(responses.GET, ITEMS_URL, json={"features": []}, status=200)
    result = sync_fetch(REPLAY_URL, TOPIC, "s", "e", 5.0)
    assert result.invalid_format is True
    assert result.messages_fetched == 0
    assert result.aborted is False


@responses.activate
def test_sync_fetch_non_json():
    responses.add(responses.GET, ITEMS_URL, body="<html>oops</html>", status=200)
    result = sync_fetch(REPLAY_URL, TOPIC, "s", "e", 5.0)
    assert result.invalid_format is True
    assert result.messages_fetched == 0


@responses.activate
def test_sync_fetch_timeout_aborts():
    responses.add(responses.GET, ITEMS_URL, body=requests.exceptions.ReadTimeout())
    result = sync_fetch(REPLAY_URL, TOPIC, "s", "e", 2.0)
    assert result.aborted is True
    assert result.invalid_format is True
    assert result.messages_fetched == 0
    assert result.fetch_delay_ms == 2.0 * 1000


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
        deadline_s=0.5, registry=registry, poll_interval=0.02,
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
        "s", "e", deadline_s=0.2, registry=registry, poll_interval=0.02,
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
        "s", "e", deadline_s=0.3, registry=registry, poll_interval=0.02,
    )
    assert result.invalid_format is True
    assert result.messages_fetched == 1
