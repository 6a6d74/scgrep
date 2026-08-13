"""Redis-backed storage of broker messages and baseline counting.

Two complementary structures are used:

* ``scgrep:msg:<id>`` — a short-lived string key per message ``id``. Written
  with ``SET NX EX`` so that duplicate ``id`` values (which legitimately arrive
  from multiple Global Brokers) are discarded, and so records expire
  automatically after ``TIME_LAG + TEST_INTERVAL + 60`` seconds.
* ``scgrep:topic:<pattern>`` — a sorted set per *configured* subscription topic,
  scored by message time. Because a received topic may match several configured
  patterns (including wildcards), a message is indexed into every pattern it
  matches. Baselines are then a simple ``ZCOUNT`` over the test window, using
  the same topic filter that is sent to the Global Replay service.
"""

from __future__ import annotations

import logging
import time

import redis

from .util import parse_time_to_epoch, topic_matches

logger = logging.getLogger(__name__)


def _parse_redis_url(url: str) -> tuple[str, int]:
    """Parse ``host:port`` (or a full ``redis://`` URL) into (host, port)."""
    if "://" in url:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        return parsed.hostname or "redis", parsed.port or 6379
    host, _, port = url.partition(":")
    return host or "redis", int(port) if port else 6379


class RedisStore:
    def __init__(
        self,
        redis_url: str,
        expiry: int,
        subscription_topics: list[str],
        client: redis.Redis | None = None,
    ) -> None:
        self._expiry = expiry
        self._topics = subscription_topics
        if client is not None:
            self._redis = client
        else:
            host, port = _parse_redis_url(redis_url)
            self._redis = redis.Redis(
                host=host, port=port, decode_responses=True
            )

    def ping(self) -> bool:
        return bool(self._redis.ping())

    def store_message(self, msg_id: str, time_value: str, received_topic: str) -> bool:
        """Store one broker message, returning True if newly stored.

        Returns False when the message is a duplicate (an ``id`` already seen)
        or when its timestamp cannot be parsed.
        """
        try:
            epoch = parse_time_to_epoch(time_value)
        except (ValueError, TypeError):
            logger.warning(
                "Discarding message %s: unparseable time %r", msg_id, time_value
            )
            return False

        # Atomic dedup + expiry. If the key already exists, this is a duplicate.
        is_new = self._redis.set(
            f"scgrep:msg:{msg_id}", received_topic, nx=True, ex=self._expiry
        )
        if not is_new:
            return False

        cutoff = time.time() - self._expiry
        for pattern in self._topics:
            if topic_matches(pattern, received_topic):
                key = f"scgrep:topic:{pattern}"
                pipe = self._redis.pipeline()
                pipe.zadd(key, {msg_id: epoch})
                pipe.zremrangebyscore(key, "-inf", cutoff)
                pipe.expire(key, self._expiry)
                pipe.execute()
        return True

    def count_messages(self, pattern: str, start_epoch: float, end_epoch: float) -> int:
        """Count messages indexed under ``pattern`` within [start, end] (inclusive)."""
        return int(
            self._redis.zcount(f"scgrep:topic:{pattern}", start_epoch, end_epoch)
        )
