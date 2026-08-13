import threading
import time

import fakeredis
import responses

from scgrep.config import Config
from scgrep.metrics import Metrics
from scgrep.redis_store import RedisStore
from scgrep.replay_registry import ReplayRegistry
from scgrep.test_cycle import run_cycle
from scgrep.util import epoch_to_iso

REPLAY_URL = "https://replay.example.org"
ITEMS_URL = f"{REPLAY_URL}/collections/wis2-notification-messages/items"
EXEC_URL = f"{REPLAY_URL}/processes/wis2-grep-subscriber/execution"
TOPIC = "monitor/a/wis2/ca-eccc-msc"
CENTRE = "ca-eccc-msc-global-replay"

ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": TOPIC,
    "GLOBAL_REPLAY_CENTRE_IDS": CENTRE,
    "GLOBAL_REPLAY_URLS": REPLAY_URL,
    "GLOBAL_BROKER_URLS": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
    "TIME_LAG": "10",
    "TEST_INTERVAL": "2",
}


def _gauge_value(metrics, gauge, **labels):
    return gauge.labels(**labels)._value.get()


@responses.activate
def test_run_cycle_publishes_all_metrics():
    cfg = Config.from_env(dict(ENV))
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", cfg.redis_expiry, cfg.subscription_topics, client=client)
    registry = ReplayRegistry()
    metrics = Metrics()

    # Anchor "now" at real wall-clock so messages stay within the expiry window.
    # Window is (now-10-2 .. now-10) = [now-12, now-10]; seed two messages there.
    now = time.time()
    in_window = epoch_to_iso(now - 11)
    store.store_message("a", in_window, TOPIC)
    store.store_message("b", in_window, TOPIC)

    channel = f"replay/a/wis2/{CENTRE}/{cfg.subscriber_id}/{TOPIC}"
    responses.add(responses.GET, ITEMS_URL, json={"numberMatched": 7}, status=200)
    responses.add(
        responses.POST, EXEC_URL,
        json={
            "subscriptions": [
                {
                    "rel": "items",
                    "type": "application/json",
                    "href": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
                    "title": "Meteo France",
                    "channel": channel,
                }
            ]
        },
        status=200,
    )

    def deliver():
        time.sleep(0.1)
        registry.handle_replay(channel)
        registry.handle_replay(channel)
        registry.handle_replay(channel)

    threading.Thread(target=deliver, daemon=True).start()

    run_cycle(cfg, store, registry, metrics, now=now)

    report_by = cfg.sensor_centre_id
    # Baseline count.
    assert _gauge_value(metrics, metrics.messages_received, report_by=report_by, topic=TOPIC) == 2

    http_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="http")
    assert _gauge_value(metrics, metrics.messages_fetched, **http_labels) == 7
    assert _gauge_value(metrics, metrics.test_aborted, **http_labels) == 0
    assert _gauge_value(metrics, metrics.response_invalid_format, **http_labels) == 0

    mqtt_labels = dict(report_by=report_by, centre_id=CENTRE, topic=TOPIC, protocol="mqtt")
    assert _gauge_value(metrics, metrics.messages_fetched, **mqtt_labels) == 3
    assert _gauge_value(metrics, metrics.test_aborted, **mqtt_labels) == 0
    assert _gauge_value(metrics, metrics.response_invalid_format, **mqtt_labels) == 0
