"""Fetch logic for testing a Global Replay service (synchronous + asynchronous).

Both functions return a :class:`FetchResult` describing exactly which metric
values to publish; the caller (``test_cycle``) applies them. This keeps the
Prometheus wiring out of the fetch code and makes the logic unit-testable.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

import requests

from .replay_registry import ReplayRegistry
from .util import broker_authority

logger = logging.getLogger(__name__)

_HEADERS = {"accept": "application/json", "Content-Type": "application/json"}


@dataclass
class FetchResult:
    """Metric values produced by a single fetch (sync or async)."""

    protocol: str
    aborted: bool
    invalid_format: bool
    fetch_delay_ms: float
    messages_fetched: int


def sync_fetch(
    replay_url: str,
    topic: str,
    start_iso: str,
    end_iso: str,
    deadline_s: float,
) -> FetchResult:
    """Synchronous fetch via OGC API - Features.

    ``deadline_s`` is 95% of ``TEST_INTERVAL``. If no response first byte is
    received within it, the test is aborted.
    """
    url = f"{replay_url}/collections/wis2-notification-messages/items"
    params = {"datetime": f"{start_iso}/{end_iso}", "topic": topic}
    aborted_delay_ms = deadline_s * 1000

    start = time.monotonic()
    try:
        # stream=True returns as soon as response headers arrive, giving a
        # good proxy for time-to-first-byte; timeout bounds it at the deadline.
        resp = requests.get(
            url, params=params, headers=_HEADERS, stream=True, timeout=deadline_s
        )
    except requests.RequestException as exc:
        logger.warning("http sync fetch aborted for %s (%s): %s", url, topic, exc)
        return FetchResult("http", True, True, aborted_delay_ms, 0)

    ttfb_ms = (time.monotonic() - start) * 1000

    try:
        body = resp.json()
    except ValueError:
        return FetchResult("http", False, True, ttfb_ms, 0)
    finally:
        resp.close()

    number_matched = body.get("numberMatched") if isinstance(body, dict) else None
    if number_matched is None:
        return FetchResult("http", False, True, ttfb_ms, 0)

    try:
        count = int(number_matched)
    except (ValueError, TypeError):
        return FetchResult("http", False, True, ttfb_ms, 0)

    return FetchResult("http", False, False, ttfb_ms, count)


def async_fetch(
    replay_url: str,
    centre_id: str,
    topic: str,
    subscriber_id: str,
    broker_authorities: list[str],
    start_iso: str,
    end_iso: str,
    deadline_s: float,
    registry: ReplayRegistry,
    poll_interval: float = 0.05,
) -> FetchResult:
    """Asynchronous fetch via OGC API - Processes, with MQTT-delivered messages.

    The expected replay channel is registered *before* the POST so that fast
    replay messages are not missed. The metadata response is validated, then the
    counter is observed until the deadline; the first-arrival time yields the
    fetch delay and the final count yields ``messages_fetched``.
    """
    # The replay service assigns a single subscriber-scoped channel (a wildcard,
    # e.g. replay/a/wis2/<centre>/<sub>/#) shared across topics and brokers, so
    # metadata is validated against this namespace prefix. The per-topic
    # expected_channel is still used to route/count replay messages.
    channel_prefix = f"replay/a/wis2/{centre_id}/{subscriber_id}/"
    expected_channel = f"{channel_prefix}{topic}"
    counter = registry.register(expected_channel)
    aborted_delay_ms = deadline_s * 1000
    start = time.monotonic()
    deadline = start + deadline_s

    url = f"{replay_url}/processes/wis2-grep-subscriber/execution"
    payload = {
        "inputs": {
            "datetime": f"{start_iso}/{end_iso}",
            "subscriber-id": subscriber_id,
            "topic": topic,
        }
    }

    invalid_format = False
    try:
        resp = requests.post(
            url, json=payload, headers=_HEADERS, timeout=deadline_s
        )
        try:
            metadata = resp.json()
        finally:
            resp.close()
        invalid_format = not _validate_subscriptions(
            metadata, channel_prefix, broker_authorities
        )
    except (requests.RequestException, ValueError) as exc:
        logger.warning("mqtt async POST failed for %s (%s): %s", url, topic, exc)
        invalid_format = True

    # Wait for the first replay message or the deadline.
    while time.monotonic() < deadline:
        _, first = counter.snapshot()
        if first is not None:
            break
        time.sleep(poll_interval)

    _, first_monotonic = counter.snapshot()
    if first_monotonic is None:
        # No message arrived before the deadline: aborted.
        registry.unregister(expected_channel)
        return FetchResult("mqtt", True, invalid_format, aborted_delay_ms, 0)

    fetch_delay_ms = (first_monotonic - start) * 1000

    # Keep counting until the deadline, then publish the count.
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)
    count, _ = counter.snapshot()
    registry.unregister(expected_channel)

    return FetchResult("mqtt", False, invalid_format, fetch_delay_ms, count)


def _validate_subscriptions(
    metadata: object, channel_prefix: str, broker_authorities: list[str]
) -> bool:
    """Validate the async metadata response's ``subscriptions`` link array.

    ``channel_prefix`` is the subscriber's replay namespace
    (``replay/a/wis2/<centre>/<subscriber>/``). Each link's ``channel`` must sit
    within it — this accepts the wildcard channel the deployed service returns
    (``…/<subscriber>/#``) as well as a per-topic channel.
    """
    if not isinstance(metadata, dict):
        return False
    subscriptions = metadata.get("subscriptions")
    if not isinstance(subscriptions, list) or not subscriptions:
        return False

    hrefs: list[str] = []
    for link in subscriptions:
        if not isinstance(link, dict):
            return False
        # Every link's channel must be within the subscriber's replay namespace.
        channel = link.get("channel")
        if not isinstance(channel, str) or not channel.startswith(channel_prefix):
            return False
        href = link.get("href")
        if not isinstance(href, str):
            return False
        hrefs.append(href)

    # Every configured Global Broker must appear as a link href.
    link_authorities = {broker_authority(h) for h in hrefs}
    for authority in broker_authorities:
        if authority not in link_authorities:
            return False

    return True
