import json
import logging
from unittest.mock import Mock

import fakeredis

from scgrep.config import BrokerConfig, Config
from scgrep.metrics import Metrics
from scgrep.mqtt_client import MessageHandler, MqttManager
from scgrep.redis_store import RedisStore
from scgrep.replay_registry import ReplayRegistry

ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": "cache/a/wis2/x/#",
    "GLOBAL_REPLAY_CENTRE_IDS": "ca-eccc",
    "GLOBAL_REPLAY_URLS": "https://replay.example.org",
    "GLOBAL_BROKER_URLS": "mqtts://everyone:everyone@globalbroker.meteo.fr:8883",
}
BROKER = BrokerConfig.from_url("mqtts://everyone:everyone@globalbroker.meteo.fr:8883")
URL = "mqtts://globalbroker.meteo.fr:8883"
REPORT_BY = "io-wis2dev-test-sensor-centre"


def _mgr():
    cfg = Config.from_env(dict(ENV))
    metrics = Metrics()
    return MqttManager(cfg, Mock(), metrics), metrics


def _status(metrics, url=URL):
    return metrics.broker_status.labels(report_by=REPORT_BY, url=url)._value.get()


def test_broker_url_strips_credentials_and_reflects_scheme():
    assert MqttManager._broker_url(BROKER) == "mqtts://globalbroker.meteo.fr:8883"
    plain = BrokerConfig.from_url("mqtt://broker.example:1883")
    assert MqttManager._broker_url(plain) == "mqtt://broker.example:1883"


def test_on_connect_success_sets_status_1():
    mgr, metrics = _mgr()
    on_connect = mgr._make_on_connect(BROKER, ["cache/a/wis2/x/#"], "Global Broker")
    client = Mock()
    on_connect(client, None, None, 0)
    assert _status(metrics) == 1
    client.subscribe.assert_called_once()  # subscribed on connect


def test_on_connect_failure_sets_status_0():
    mgr, metrics = _mgr()
    on_connect = mgr._make_on_connect(BROKER, [], "Global Broker")
    on_connect(Mock(), None, None, 5)  # non-zero reason code = failed connect
    assert _status(metrics) == 0


def test_on_disconnect_sets_status_0():
    mgr, metrics = _mgr()
    mgr._make_on_connect(BROKER, [], "Global Broker")(Mock(), None, None, 0)
    assert _status(metrics) == 1
    mgr._make_on_disconnect(BROKER)(Mock(), None)
    assert _status(metrics) == 0


def test_status_helper_noop_without_metrics():
    cfg = Config.from_env(dict(ENV))
    mgr = MqttManager(cfg, Mock(), metrics=None)
    # Must not raise when no metrics object was provided.
    mgr._set_broker_status(BROKER, True)


# --------------------------------------------------------------------------
# MessageHandler DEBUG logging (broker attribution + Duplicate? flag)
# --------------------------------------------------------------------------

class _Msg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


def _handler():
    client = fakeredis.FakeRedis(decode_responses=True)
    store = RedisStore("redis:6379", 300, ["cache/a/wis2/x/#"], client=client)
    return MessageHandler(store, ReplayRegistry())


def _payload(mid="m1"):
    return json.dumps(
        {"id": mid, "properties": {"pubtime": "2026-08-17T10:00:00Z"}}
    ).encode()


def test_on_message_debug_logs_broker_url_and_duplicate(caplog):
    handler = _handler()
    url = "mqtts://gb.wis2dev.io:8883"
    msg = _Msg("cache/a/wis2/x/data/core/y", _payload())
    with caplog.at_level(logging.DEBUG, logger="scgrep.mqtt_client"):
        handler.on_message(None, url, msg)  # first arrival
        handler.on_message(None, url, msg)  # duplicate
    text = caplog.text
    assert f"MQTT message received: broker={url}" in text
    assert "Duplicate?=false" in text  # first time
    assert "Duplicate?=true" in text   # second time (discarded)


def test_on_message_replay_debug_log_attributes_broker(caplog):
    handler = _handler()
    url = "mqtts://wis2-grep.weather.gc.ca:8883"
    topic = "replay/a/wis2/ca-eccc/uuid/cache/a/wis2/x/data/core/y"
    with caplog.at_level(logging.DEBUG, logger="scgrep.mqtt_client"):
        handler.on_message(None, url, _Msg(topic, _payload("r1")))
    text = caplog.text
    assert f"broker={url}" in text and "Duplicate?=false" in text
    assert "Replay message (asynchronous)" in text  # INFO line still emitted
