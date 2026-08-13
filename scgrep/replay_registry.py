"""Thread-safe registry correlating async replay MQTT messages to active tests.

When an asynchronous replay test is started it registers the MQTT *channel* it
expects the Global Replay service to publish on. Replay messages arrive on the
shared paho network thread; :meth:`ReplayRegistry.handle_replay` routes each one
to the matching :class:`ReplayCounter`, recording the first-arrival time (for the
fetch-delay metric) and a running count.
"""

from __future__ import annotations

import threading
import time

from .util import topic_matches


class ReplayCounter:
    """First-arrival timing and message count for one active async test."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.count = 0
        self.first_monotonic: float | None = None

    def record(self, now: float) -> None:
        with self._lock:
            self.count += 1
            if self.first_monotonic is None:
                self.first_monotonic = now

    def snapshot(self) -> tuple[int, float | None]:
        with self._lock:
            return self.count, self.first_monotonic


class ReplayRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: dict[str, ReplayCounter] = {}

    def register(self, channel: str) -> ReplayCounter:
        """Begin tracking replay messages for ``channel``; returns its counter."""
        counter = ReplayCounter()
        with self._lock:
            self._active[channel] = counter
        return counter

    def unregister(self, channel: str) -> None:
        with self._lock:
            self._active.pop(channel, None)

    def handle_replay(self, topic: str) -> None:
        """Route an incoming replay message (called from the MQTT thread)."""
        now = time.monotonic()
        with self._lock:
            items = list(self._active.items())
        for channel, counter in items:
            if _channel_matches(channel, topic):
                counter.record(now)
                return


def _channel_matches(channel: str, topic: str) -> bool:
    """Match an incoming replay ``topic`` against a registered ``channel``.

    Handles an exact match, a subtopic beneath the channel, and MQTT wildcards
    embedded in the channel (when the evaluated topic itself contained ``+``/``#``).
    """
    if topic == channel:
        return True
    if topic.startswith(channel.rstrip("#").rstrip("/") + "/"):
        return True
    return topic_matches(channel, topic)
