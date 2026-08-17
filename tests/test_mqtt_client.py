from unittest.mock import Mock

from scgrep.config import BrokerConfig, Config
from scgrep.metrics import Metrics
from scgrep.mqtt_client import MqttManager

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
