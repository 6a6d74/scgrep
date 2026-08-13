import time

from scgrep.config import Config
from scgrep.main import _wait_for_redis

BASE_ENV = {
    "SENSOR_CENTRE_ID": "io-wis2dev-test-sensor-centre",
    "SUBSCRIPTION_TOPICS": "monitor/a/wis2/ca-eccc-msc",
}


class _FlakyStore:
    """Fails ``fail_times`` pings, then succeeds."""

    def __init__(self, fail_times: int) -> None:
        self.remaining = fail_times
        self.calls = 0

    def ping(self) -> bool:
        self.calls += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise ConnectionError("not ready")
        return True


class _DeadStore:
    def ping(self) -> bool:
        raise ConnectionError("never ready")


def test_redis_startup_timeout_default():
    cfg = Config.from_env(dict(BASE_ENV))
    assert cfg.redis_startup_timeout == 60


def test_redis_startup_timeout_override():
    cfg = Config.from_env(dict(BASE_ENV, REDIS_STARTUP_TIMEOUT="5"))
    assert cfg.redis_startup_timeout == 5


def test_wait_for_redis_retries_then_succeeds():
    store = _FlakyStore(fail_times=2)
    assert _wait_for_redis(store, "redis:6379", timeout=5, interval=0.01) is True
    assert store.calls == 3


def test_wait_for_redis_times_out():
    store = _DeadStore()
    start = time.monotonic()
    assert _wait_for_redis(store, "redis:6379", timeout=0, interval=0.01) is False
    # timeout=0 means it should give up on the first failed attempt.
    assert time.monotonic() - start < 1
