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
from urllib.parse import urljoin

import requests

from .redis_store import RedisStore
from .replay_registry import ReplayRegistry
from .util import broker_authority

logger = logging.getLogger(__name__)

_HEADERS = {"accept": "application/json", "Content-Type": "application/json"}

# Safety cap on synchronous paging, so a malformed `next` link cannot loop
# forever (the fetch deadline also bounds paging).
_MAX_PAGES = 10_000


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
    # http only: the count of features actually returned did not match
    # numberMatched. Always False for mqtt.
    invalid_number_matched: bool = False


def _next_page_href(page: object) -> str | None:
    """Return the ``rel: next`` link href from a Feature Collection, or None."""
    if not isinstance(page, dict):
        return None
    links = page.get("links")
    if not isinstance(links, list):
        return None
    for link in links:
        if isinstance(link, dict) and link.get("rel") == "next":
            href = link.get("href")
            if isinstance(href, str) and href:
                return href
    return None


def sync_fetch(
    replay_url: str,
    centre_id: str,
    topic: str,
    start_iso: str,
    end_iso: str,
    deadline_s: float,
    store: RedisStore,
    max_chars: int = 1000,
    deadline_at: float | None = None,
) -> FetchResult:
    """Synchronous fetch via OGC API - Features.

    Retrieves the full result set, paging through ``rel: next`` links, and for
    every returned Feature: counts it, records it in Redis (via
    ``store_sync_message``), and logs it. ``numberMatched`` is read from the first
    page only; when all pages are in, the count of returned messages is compared
    with ``numberMatched`` and any mismatch is flagged (and logged as an error).

    ``deadline_s`` is 95% of ``TEST_INTERVAL`` and sets the aborted fetch-delay
    value. ``deadline_at`` is an absolute ``time.monotonic()`` instant at which to
    stop; when omitted it defaults to ``now + deadline_s``. A shared
    ``deadline_at`` lets every fetch in a cycle stop at the same moment.
    ``max_chars`` caps how much of the HTTP response body is logged.
    """
    interval = f"{start_iso}/{end_iso}"
    base_url = f"{replay_url}/collections/wis2-notification-messages/items"
    first_url = f"{base_url}?datetime={interval}&topic={topic}"
    aborted_delay_ms = deadline_s * 1000

    start = time.monotonic()
    if deadline_at is None:
        deadline_at = start + deadline_s

    def log_request(request_line: str) -> None:
        logger.info(
            "Global Replay request: centre_id=%s type=synchronous topic=%s "
            "interval=%s request=%s",
            centre_id, topic, interval, request_line,
        )

    def log_response(body_text: str) -> None:
        logger.info(
            "Global Replay response: centre_id=%s type=synchronous topic=%s "
            "response=%s",
            centre_id, topic, _truncate(body_text, max_chars),
        )

    # --- first page: measure time-to-first-byte and read numberMatched -------
    log_request(f"GET {first_url}")
    try:
        resp = requests.get(
            base_url, params={"datetime": interval, "topic": topic},
            headers=_HEADERS, stream=True, timeout=max(0.05, deadline_at - start),
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
    log_response(body_text)

    try:
        page = json.loads(body_text)
    except ValueError:
        logger.warning(
            "Global Replay synchronous response is not JSON: centre_id=%s topic=%s",
            centre_id, topic,
        )
        return FetchResult("http", False, True, ttfb_ms, 0)

    number_matched = page.get("numberMatched") if isinstance(page, dict) else None
    if number_matched is None:
        logger.warning(
            "Global Replay synchronous response missing numberMatched: "
            "centre_id=%s topic=%s",
            centre_id, topic,
        )
        return FetchResult("http", False, True, ttfb_ms, 0)
    try:
        number_matched = int(number_matched)
    except (ValueError, TypeError):
        logger.warning(
            "Global Replay synchronous numberMatched not an integer (%r): "
            "centre_id=%s topic=%s",
            number_matched, centre_id, topic,
        )
        return FetchResult("http", False, True, ttfb_ms, 0)

    logger.info(
        "Global Replay numberMatched: centre_id=%s topic=%s numberMatched=%d",
        centre_id, topic, number_matched,
    )

    # --- walk every page, counting/storing/logging each returned message -----
    messages_seen = 0
    page_url = first_url
    for _ in range(_MAX_PAGES):
        messages_seen += _process_features(store, centre_id, topic, page)

        next_href = _next_page_href(page)
        if not next_href:
            break
        if time.monotonic() >= deadline_at:
            logger.warning(
                "Global Replay synchronous paging stopped at the deadline: "
                "centre_id=%s topic=%s messages_seen=%d",
                centre_id, topic, messages_seen,
            )
            break
        next_url = urljoin(page_url, next_href)
        log_request(f"GET {next_url}")
        try:
            resp = requests.get(
                next_url, headers=_HEADERS,
                timeout=max(0.05, deadline_at - time.monotonic()),
            )
            body_text = resp.text
            resp.close()
            log_response(body_text)
            page = json.loads(body_text)
        except (requests.RequestException, ValueError) as exc:
            logger.warning(
                "Global Replay synchronous next-page request failed: "
                "centre_id=%s topic=%s reason=%s",
                centre_id, topic, exc,
            )
            break
        page_url = next_url

    invalid_number_matched = messages_seen != number_matched
    if invalid_number_matched:
        logger.error(
            "Global Replay synchronous numberMatched mismatch: centre_id=%s "
            "topic=%s numberMatched=%d messages_seen=%d",
            centre_id, topic, number_matched, messages_seen,
        )

    return FetchResult(
        "http", False, False, ttfb_ms, number_matched,
        invalid_number_matched=invalid_number_matched,
    )


def _process_features(store: RedisStore, centre_id: str, topic: str, page: object) -> int:
    """Count, store and log every Feature on a page; return the count."""
    features = page.get("features") if isinstance(page, dict) else None
    if not isinstance(features, list):
        return 0
    seen = 0
    for feature in features:
        seen += 1
        if not isinstance(feature, dict):
            continue
        msg_id = feature.get("id")
        time_value = feature.get("time")
        if time_value is None:
            props = feature.get("properties")
            if isinstance(props, dict):
                time_value = props.get("pubtime")
        if not msg_id or not time_value:
            logger.warning(
                "Global Replay synchronous feature missing id/time: "
                "centre_id=%s topic=%s",
                centre_id, topic,
            )
            continue
        store.store_sync_message(centre_id, topic, str(msg_id), str(time_value))
        logger.info(
            "Replay message (synchronous): centre_id=%s topic=%s id=%s time=%s",
            centre_id, topic, msg_id, time_value,
        )
    return seen


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
