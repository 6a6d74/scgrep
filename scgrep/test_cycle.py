"""Orchestration of a single test cycle across all topics and replay services."""

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor

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
    Baselines are read from Redis, then every (topic, replay-service) pair is
    tested with a synchronous and an asynchronous fetch, all in parallel so the
    cycle completes within one ``TEST_INTERVAL``.
    """
    now = time.time() if now is None else now
    end_epoch = now - config.time_lag
    start_epoch = end_epoch - config.test_interval
    start_iso = epoch_to_iso(start_epoch)
    end_iso = epoch_to_iso(end_epoch)
    deadline_s = 0.95 * config.test_interval
    report_by = config.sensor_centre_id
    broker_authorities = [broker_authority(b.url) for b in config.brokers]

    logger.info("Running test cycle for window %s .. %s", start_iso, end_iso)

    # Baselines: fast Redis reads, published up front.
    for topic in config.subscription_topics:
        count = store.count_messages(topic, start_epoch, end_epoch)
        metrics.messages_received.labels(report_by=report_by, topic=topic).set(count)

    # Fan out all fetches (the slow part) in parallel.
    max_workers = max(1, len(config.subscription_topics) * len(config.replay_targets) * 2)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fetch") as ex:
        futures = []
        for topic in config.subscription_topics:
            for centre_id, replay_url in config.replay_targets:
                futures.append(
                    ex.submit(
                        _run_and_publish_sync,
                        metrics, report_by, centre_id, topic,
                        replay_url, start_iso, end_iso, deadline_s,
                    )
                )
                futures.append(
                    ex.submit(
                        _run_and_publish_async,
                        metrics, report_by, centre_id, topic, replay_url,
                        config.subscriber_id, broker_authorities,
                        start_iso, end_iso, deadline_s, registry,
                    )
                )
        for future in futures:
            try:
                future.result()
            except Exception:  # noqa: BLE001 - one failure must not sink the cycle
                logger.exception("A fetch task raised an unexpected error")

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


def _run_and_publish_sync(
    metrics, report_by, centre_id, topic, replay_url, start_iso, end_iso, deadline_s
) -> None:
    result = sync_fetch(replay_url, topic, start_iso, end_iso, deadline_s)
    _publish(metrics, report_by, centre_id, topic, result)


def _run_and_publish_async(
    metrics, report_by, centre_id, topic, replay_url, subscriber_id,
    broker_authorities, start_iso, end_iso, deadline_s, registry
) -> None:
    result = async_fetch(
        replay_url, centre_id, topic, subscriber_id, broker_authorities,
        start_iso, end_iso, deadline_s, registry,
    )
    _publish(metrics, report_by, centre_id, topic, result)
