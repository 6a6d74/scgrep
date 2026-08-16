"""Fetch logic for testing a Global Replay service (synchronous + asynchronous).

Both functions return a :class:`FetchResult` describing exactly which metric
values to publish; the caller (``test_cycle``) applies them. This keeps the
Prometheus wiring out of the fetch code and makes the logic unit-testable.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass

import requests

from .replay_registry import ReplayRegistry
from .util import broker_authority

logger = logging.getLogger(__name__)

_HEADERS = {"accept": "application/json", "Content-Type": "application/json"}


def _truncate(text: str, max_chars: int) -> str:
    """Truncate ``text`` for logging, noting the original length when clipped."""
    if len(text) <= max_chars:
        return text
    return f"{text[:max_chars]}... [truncated, {len(text)} chars total]"


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
    centre_id: str,
    topic: str,
    start_iso: str,
    end_iso: str,
    deadline_s: float,
    max_chars: int = 1000,
    deadline_at: float | None = None,
) -> FetchResult:
    """Synchronous fetch via OGC API - Features.

    ``deadline_s`` is 95% of ``TEST_INTERVAL`` and sets the aborted fetch-delay
    value. ``deadline_at`` is an absolute ``time.monotonic()`` instant at which
    to stop; when omitted it defaults to ``now + deadline_s``. Passing a shared
    ``deadline_at`` lets every fetch in a cycle stop at the same moment.
    ``max_chars`` caps how much of the HTTP response body is logged.
    """
    url = f"{replay_url}/collections/wis2-notification-messages/items"
    params = {"datetime": f"{start_iso}/{end_iso}", "topic": topic}
    aborted_delay_ms = deadline_s * 1000
    interval = f"{start_iso}/{end_iso}"

    logger.info(
        "Global Replay request: centre_id=%s type=synchronous topic=%s interval=%s "
        "request=%s",
        centre_id, topic, interval,
        f"GET {url}?datetime={interval}&topic={topic}",
    )

    start = time.monotonic()
    if deadline_at is None:
        deadline_at = start + deadline_s
    timeout = max(0.05, deadline_at - start)
    try:
        # stream=True returns as soon as response headers arrive, giving a
        # good proxy for time-to-first-byte; timeout bounds it at the deadline.
        resp = requests.get(
            url, params=params, headers=_HEADERS, stream=True, timeout=timeout
        )
    except requests.RequestException as exc:
        logger.warning(
            "Global Replay synchronous test aborted: centre_id=%s topic=%s reason=%s",
            centre_id, topic, exc,
        )
        return FetchResult("http", True, True, aborted_delay_ms, 0)

    ttfb_ms = (time.monotonic() - start) * 1000

    try:
        body_text = resp.text
    finally:
        resp.close()
    logger.info(
        "Global Replay response: centre_id=%s type=synchronous topic=%s response=%s",
        centre_id, topic, _truncate(body_text, max_chars),
    )

    try:
        body = json.loads(body_text)
    except ValueError:
        logger.warning(
            "Global Replay synchronous response is not JSON: centre_id=%s topic=%s",
            centre_id, topic,
        )
        return FetchResult("http", False, True, ttfb_ms, 0)

    number_matched = body.get("numberMatched") if isinstance(body, dict) else None
    if number_matched is None:
        logger.warning(
            "Global Replay synchronous response missing numberMatched: "
            "centre_id=%s topic=%s",
            centre_id, topic,
        )
        return FetchResult("http", False, True, ttfb_ms, 0)

    try:
        count = int(number_matched)
    except (ValueError, TypeError):
        logger.warning(
            "Global Replay synchronous numberMatched not an integer (%r): "
            "centre_id=%s topic=%s",
            number_matched, centre_id, topic,
        )
        return FetchResult("http", False, True, ttfb_ms, 0)

    logger.info(
        "Global Replay numberMatched: centre_id=%s topic=%s numberMatched=%d",
        centre_id, topic, count,
    )
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
    baseline: int,
    max_chars: int = 1000,
    poll_interval: float = 0.05,
    deadline_at: float | None = None,
) -> FetchResult:
    """Asynchronous fetch via OGC API - Processes, with MQTT-delivered messages.

    The expected replay channel is registered *before* the POST so that fast
    replay messages are not missed. The metadata response is validated, then the
    counter is observed until the deadline; the first-arrival time yields the
    fetch delay and the final count yields ``messages_fetched``.

    ``baseline`` is the number of messages the Sensor Centre received for this
    topic/window. When it is **zero**, no replay messages are expected, so waiting
    for MQTT would always time out; instead the fetch reports the time to the
    first byte of its HTTP (process-execution) response as ``fetch_delay`` and
    does not abort. When it is non-zero, the wait-for-first-message logic applies.

    ``deadline_s`` is 95% of ``TEST_INTERVAL`` (aborted fetch-delay value);
    ``deadline_at`` is an absolute ``time.monotonic()`` stop instant, defaulting
    to ``now + deadline_s``. A shared ``deadline_at`` makes every fetch in a cycle
    stop simultaneously.
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
    deadline = start + deadline_s if deadline_at is None else deadline_at

    interval = f"{start_iso}/{end_iso}"
    url = f"{replay_url}/processes/wis2-grep-subscriber/execution"
    payload = {
        "inputs": {
            "datetime": interval,
            "subscriber-id": subscriber_id,
            "topic": topic,
        }
    }

    logger.info(
        "Global Replay request: centre_id=%s type=asynchronous topic=%s interval=%s "
        "request=%s payload=%s",
        centre_id, topic, interval, f"POST {url}", json.dumps(payload),
    )

    invalid_format = False
    http_response_at: float | None = None
    try:
        resp = requests.post(
            url, json=payload, headers=_HEADERS,
            timeout=max(0.05, deadline - time.monotonic()),
        )
        http_response_at = time.monotonic()
        try:
            body_text = resp.text
        finally:
            resp.close()
        logger.info(
            "Global Replay response: centre_id=%s type=asynchronous topic=%s "
            "response=%s",
            centre_id, topic, _truncate(body_text, max_chars),
        )
        metadata = json.loads(body_text)
        invalid_format = not _validate_subscriptions(
            metadata, channel_prefix, broker_authorities
        )
        if invalid_format:
            logger.warning(
                "Global Replay asynchronous metadata failed validation: "
                "centre_id=%s topic=%s",
                centre_id, topic,
            )
    except (requests.RequestException, ValueError) as exc:
        logger.warning(
            "Global Replay asynchronous POST failed: centre_id=%s topic=%s reason=%s",
            centre_id, topic, exc,
        )
        invalid_format = True
        if http_response_at is None:
            http_response_at = time.monotonic()

    # Zero baseline: no replay messages are expected, so waiting for MQTT would
    # always abort. Report the HTTP response delay instead and do not abort.
    if baseline == 0:
        registry.unregister(expected_channel)
        fetch_delay_ms = (http_response_at - start) * 1000
        return FetchResult("mqtt", False, invalid_format, fetch_delay_ms, 0)

    # Non-zero baseline: wait for the first replay message or the deadline.
    while time.monotonic() < deadline:
        _, first = counter.snapshot()
        if first is not None:
            break
        time.sleep(poll_interval)

    _, first_monotonic = counter.snapshot()
    if first_monotonic is None:
        # No message arrived before the deadline: aborted.
        registry.unregister(expected_channel)
        logger.warning(
            "Global Replay asynchronous test aborted (no replay message within "
            "the deadline): centre_id=%s topic=%s",
            centre_id, topic,
        )
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
