import time

import fakeredis
import pytest

from scgrep.redis_store import RedisStore
from scgrep.util import epoch_to_iso

TOPICS = [
    "cache/a/wis2/ca-eccc-msc/data/#",
    "monitor/a/wis2/ca-eccc-msc",
]


def iso_ago(seconds: float) -> str:
    """An ISO timestamp `seconds` in the past (kept within the expiry window)."""
    return epoch_to_iso(time.time() - seconds)


@pytest.fixture
def store():
    client = fakeredis.FakeRedis(decode_responses=True)
    return RedisStore("redis:6379", expiry=660, subscription_topics=TOPICS, client=client)


def test_store_and_count(store):
    assert store.store_message("id-1", iso_ago(60), "cache/a/wis2/ca-eccc-msc/data/core/x")
    assert store.store_message("id-2", iso_ago(50), "cache/a/wis2/ca-eccc-msc/data/core/y")
    now = time.time()
    count = store.count_messages("cache/a/wis2/ca-eccc-msc/data/#", now - 600, now)
    assert count == 2


def test_duplicate_id_discarded(store):
    assert store.store_message("dup", iso_ago(60), "cache/a/wis2/ca-eccc-msc/data/x")
    assert not store.store_message("dup", iso_ago(30), "cache/a/wis2/ca-eccc-msc/data/x")
    now = time.time()
    count = store.count_messages("cache/a/wis2/ca-eccc-msc/data/#", now - 600, now)
    assert count == 1


def test_window_filtering(store):
    now = time.time()
    store.store_message("early", iso_ago(200), "monitor/a/wis2/ca-eccc-msc")
    store.store_message("inside", iso_ago(60), "monitor/a/wis2/ca-eccc-msc")
    # Window covers only the last 120s, so only "inside" is counted.
    assert store.count_messages("monitor/a/wis2/ca-eccc-msc", now - 120, now) == 1


def test_message_indexed_under_matching_pattern_only(store):
    now = time.time()
    store.store_message("m", iso_ago(60), "monitor/a/wis2/ca-eccc-msc")
    assert store.count_messages("monitor/a/wis2/ca-eccc-msc", now - 600, now) == 1
    assert store.count_messages("cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 0


def test_unparseable_time_discarded(store):
    now = time.time()
    assert not store.store_message("bad", "not-a-time", "monitor/a/wis2/ca-eccc-msc")
    assert store.count_messages("monitor/a/wis2/ca-eccc-msc", now - 600, now) == 0


def test_replay_dedup_across_brokers(store):
    now = time.time()
    iso = iso_ago(60)
    # Same id delivered by two brokers -> counted once (sorted-set member unique).
    store.store_replay_message("c1", "id-1", iso, "cache/a/wis2/ca-eccc-msc/data/x")
    store.store_replay_message("c1", "id-1", iso, "cache/a/wis2/ca-eccc-msc/data/x")
    store.store_replay_message("c1", "id-2", iso, "cache/a/wis2/ca-eccc-msc/data/y")
    assert store.count_replay_messages("c1", "cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 2


def test_replay_centre_scoped(store):
    now = time.time()
    iso = iso_ago(60)
    # Different Global Replay services replay the same id -> counted per centre.
    store.store_replay_message("c1", "id-1", iso, "cache/a/wis2/ca-eccc-msc/data/x")
    store.store_replay_message("c2", "id-1", iso, "cache/a/wis2/ca-eccc-msc/data/x")
    assert store.count_replay_messages("c1", "cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 1
    assert store.count_replay_messages("c2", "cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 1


def test_replay_separate_from_baseline(store):
    now = time.time()
    iso = iso_ago(60)
    # A replay carries the same id as the baseline; the two keyspaces must not clash.
    store.store_message("id-1", iso, "cache/a/wis2/ca-eccc-msc/data/x")
    store.store_replay_message("c1", "id-1", iso, "cache/a/wis2/ca-eccc-msc/data/x")
    assert store.count_messages("cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 1
    assert store.count_replay_messages("c1", "cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 1


def test_replay_clear(store):
    now = time.time()
    store.store_replay_message("c1", "id-1", iso_ago(60), "cache/a/wis2/ca-eccc-msc/data/x")
    store.clear_replay(["c1"])
    assert store.count_replay_messages("c1", "cache/a/wis2/ca-eccc-msc/data/#", now - 600, now) == 0


def test_replay_window_filtering(store):
    now = time.time()
    store.store_replay_message("c1", "old", iso_ago(200), "monitor/a/wis2/ca-eccc-msc")
    store.store_replay_message("c1", "new", iso_ago(60), "monitor/a/wis2/ca-eccc-msc")
    assert store.count_replay_messages("c1", "monitor/a/wis2/ca-eccc-msc", now - 120, now) == 1
