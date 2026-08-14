"""Orchestration of a single test cycle across all topics and replay services."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

from .config import Config
from .metrics import Metrics
from .redis_store import RedisStore
from .replay_registry import ReplayRegistry
from .replay_tester import FetchResult, async_fetch, sync_fetch
from .util import broker_authority, epoch_to_iso

logger = logging.getLogger(__name__)


def run_cycle(
    config: Config,
    store: RedisStore,
    registry: ReplayRegistry,
    metrics: Metrics,
    now: float | None = None,
) -> None:
    """Run one full test cycle.

    The test window is ``(now - TIME_LAG - TEST_INTERVAL) .. (now - TIME_LAG)``.
    Baselines are read from Redis and every (topic, replay-service) pair is tested
    with a synchronous and an asynchronous fetch, all in parallel so the cycle
    completes within one ``TEST_INTERVAL``.

    Every metric for the cycle is published together, at exactly 95% of the test
    period. All fetches share a single absolute deadline anchored to the start of
    the cycle, and publication is held to that same instant — so the baseline,
    the synchronous (``http``) results, and the asynchronous (``mqtt``) results
    all update at once and a single Prometheus scrape sees a consistent set.
    """
    now = time.time() if now is None else now
    cycle_start = time.monotonic()
    end_epoch = now - config.time_lag
    start_epoch = end_epoch - config.test_interval
    start_iso = epoch_to_iso(start_epoch)
    end_iso = epoch_to_iso(end_epoch)
    deadline_s = 0.95 * config.test_interval
    # Absolute instant, 95% through the test period, at which all fetches stop
    # and all metrics are published.
    deadline_at = cycle_start + deadline_s
    report_by = config.sensor_centre_id
    broker_authorities = [broker_authority(b.url) for b in config.brokers]

    logger.info("Running test cycle for window %s .. %s", start_iso, end_iso)

    # Baselines: fast Redis reads, held for publication at the deadline.
    baselines = {
        topic: store.count_messages(topic, start_epoch, end_epoch)
        for topic in config.subscription_topics
    }

    # Clean sheet for this cycle's replay counting, before any process is
    # triggered, so replay messages are counted (deduplicated across brokers)
    # against a fresh set.
    store.clear_replay(config.replay_centre_ids)

    # Fan out all fetches (the slow part) in parallel and collect their results;
    # nothing is published until every fetch has completed.
    max_workers = max(1, len(config.subscription_topics) * len(config.replay_targets) * 2)
    fetch_results: list[tuple[str, str, FetchResult]] = []
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fetch") as ex:
        futures = {}
        for topic in config.subscription_topics:
            for centre_id, replay_url in config.replay_targets:
                futures[ex.submit(
                    sync_fetch, replay_url, topic, start_iso, end_iso,
                    deadline_s, deadline_at,
                )] = (centre_id, topic)
                futures[ex.submit(
                    async_fetch, replay_url, centre_id, topic, config.subscriber_id,
                    broker_authorities, start_iso, end_iso, deadline_s, registry,
                    baselines[topic], deadline_at=deadline_at,
                )] = (centre_id, topic)
        for future, (centre_id, topic) in futures.items():
            try:
                result = future.result()
            except Exception:  # noqa: BLE001 - one failure must not sink the cycle
                logger.exception("A fetch task raised an unexpected error")
                continue
            # The async (mqtt) count comes from Redis: deduplicated across replay
            # brokers, counted the same way as the baseline. The registry is used
            # only for timing/abort, so its (non-deduplicated) count is replaced.
            if result.protocol == "mqtt" and not result.aborted and baselines[topic] > 0:
                deduped = store.count_replay_messages(
                    centre_id, topic, start_epoch, end_epoch
                )
                result = replace(result, messages_fetched=deduped)
            fetch_results.append((centre_id, topic, result))

    # Hold publication to exactly 95% of the test period. Fetches normally run
    # right up to this instant; this also guards the case where they finish early
    # (e.g. errors), so every cycle publishes at the same point.
    remaining = deadline_at - time.monotonic()
    if remaining > 0:
        time.sleep(remaining)

    # Publish everything together so all metrics for this cycle update at once.
    for topic, count in baselines.items():
        metrics.messages_received.labels(report_by=report_by, topic=topic).set(count)
    for centre_id, topic, result in fetch_results:
        _publish(metrics, report_by, centre_id, topic, result)

    logger.info("Test cycle complete")


def _publish(
    metrics: Metrics,
    report_by: str,
    centre_id: str,
    topic: str,
    result: FetchResult,
) -> None:
    labels = dict(
        report_by=report_by,
        centre_id=centre_id,
        topic=topic,
        protocol=result.protocol,
    )
    metrics.test_aborted.labels(**labels).set(1 if result.aborted else 0)
    metrics.response_invalid_format.labels(**labels).set(
        1 if result.invalid_format else 0
    )
    metrics.fetch_delay.labels(**labels).set(result.fetch_delay_ms)
    metrics.messages_fetched.labels(**labels).set(result.messages_fetched)
