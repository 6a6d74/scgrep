import pytest

from scgrep.config import BrokerConfig, Config, ConfigError

BASE_ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": "cache/a/wis2/ca-eccc-msc/data/#,monitor/a/wis2/ca-eccc-msc",
}


def test_defaults_applied():
    cfg = Config.from_env(dict(BASE_ENV))
    assert cfg.redis_url == "redis:6379"
    assert cfg.time_lag == 300
    assert cfg.test_interval == 300
    assert cfg.metrics_endpoint == "/metrics"
    assert cfg.replay_centre_ids == ["ca-eccc-msc-global-replay"]
    assert cfg.replay_urls == ["https://wis2-grep.weather.gc.ca"]
    assert len(cfg.brokers) == 1
    assert cfg.redis_expiry == 300 + 300 + 60


def test_replay_broker_default():
    cfg = Config.from_env(dict(BASE_ENV))
    assert cfg.replay_broker.host == "wis2-grep.weather.gc.ca"
    assert cfg.replay_broker.port == 8883
    assert cfg.replay_broker.tls is True
    assert cfg.replay_broker_tls_insecure is True


def test_replay_broker_override():
    env = dict(
        BASE_ENV,
        GLOBAL_REPLAY_BROKER_URL="mqtts://u:p@replay.example:8883",
        GLOBAL_REPLAY_BROKER_TLS_INSECURE="false",
    )
    cfg = Config.from_env(env)
    assert cfg.replay_broker.host == "replay.example"
    assert cfg.replay_broker.username == "u"
    assert cfg.replay_broker_tls_insecure is False


def test_subscriber_id_is_uuid_like():
    cfg = Config.from_env(dict(BASE_ENV))
    assert len(cfg.subscriber_id) == 36
    assert cfg.subscriber_id.count("-") == 4


def test_missing_sensor_centre_id():
    env = dict(BASE_ENV)
    del env["SENSOR_CENTRE_ID"]
    with pytest.raises(ConfigError):
        Config.from_env(env)


def test_missing_subscription_topics():
    env = dict(BASE_ENV)
    del env["SUBSCRIPTION_TOPICS"]
    with pytest.raises(ConfigError):
        Config.from_env(env)


def test_replay_list_length_mismatch():
    env = dict(BASE_ENV)
    env["GLOBAL_REPLAY_CENTRE_IDS"] = "a,b"
    env["GLOBAL_REPLAY_URLS"] = "https://one"
    with pytest.raises(ConfigError):
        Config.from_env(env)


def test_replay_urls_trailing_slash_stripped():
    env = dict(BASE_ENV)
    env["GLOBAL_REPLAY_URLS"] = "https://example.org/"
    cfg = Config.from_env(env)
    assert cfg.replay_urls == ["https://example.org"]


def test_metrics_endpoint_normalised():
    env = dict(BASE_ENV, METRICS_ENDPOINT="stats")
    cfg = Config.from_env(env)
    assert cfg.metrics_endpoint == "/stats"


def test_replay_wildcard_topics():
    cfg = Config.from_env(dict(BASE_ENV))
    topics = cfg.replay_wildcard_topics()
    assert topics == [
        f"replay/a/wis2/ca-eccc-msc-global-replay/{cfg.subscriber_id}/#"
    ]


def test_broker_url_parsing():
    b = BrokerConfig.from_url("mqtts://user:pass@host.example:8883")
    assert b.host == "host.example"
    assert b.port == 8883
    assert b.username == "user"
    assert b.password == "pass"
    assert b.tls is True


def test_broker_url_defaults_port_for_plain_mqtt():
    b = BrokerConfig.from_url("mqtt://host.example")
    assert b.port == 1883
    assert b.tls is False


def test_broker_url_rejects_bad_scheme():
    with pytest.raises(ConfigError):
        BrokerConfig.from_url("http://host.example")
