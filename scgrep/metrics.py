"""Prometheus metric definitions and the HTTP server that exposes them."""

from __future__ import annotations

import logging
import threading
from wsgiref.simple_server import WSGIRequestHandler, make_server

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    disable_created_metrics,
    make_wsgi_app,
)

logger = logging.getLogger(__name__)

_METRIC_PREFIX = "wmo_wis2_scgrep_"

# The message counters are true Counters; suppress the companion ``_created``
# timestamp series so the exposition stays limited to the documented metrics.
disable_created_metrics()


class Metrics:
    """Container for the SCGRep Prometheus metrics (counters and gauges).

    A dedicated :class:`~prometheus_client.CollectorRegistry` is used so the
    metrics are isolated from the process-global default registry (which keeps
    tests hermetic and avoids duplicate-registration errors).
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()

        # Cumulative counters (never reset per cycle): each test period increments
        # them by that period's count, so they grow monotonically. The name is
        # given without the ``_total`` suffix because prometheus_client appends it
        # for counters, yielding e.g. ``..._messages_received_total`` in the
        # exposition. Use increase(...[60s]) in queries to recover a per-interval
        # value.
        self.messages_received = Counter(
            _METRIC_PREFIX + "messages_received",
            "Cumulative messages received from Global Brokers on the topic; "
            "incremented by each test period's count",
            ["report_by", "topic"],
            registry=self.registry,
        )
        self.messages_fetched = Counter(
            _METRIC_PREFIX + "messages_fetched",
            "Cumulative messages retrieved from the Global Replay service on the "
            "topic; incremented by each test period's count",
            ["report_by", "centre_id", "topic", "protocol"],
            registry=self.registry,
        )
        self.test_aborted = Gauge(
            _METRIC_PREFIX + "test_aborted_flag",
            "Set to 1 if the test was aborted because retrieval exceeded the "
            "test period",
            ["report_by", "centre_id", "topic", "protocol"],
            registry=self.registry,
        )
        self.fetch_delay = Gauge(
            _METRIC_PREFIX + "fetch_delay_time",
            "Milliseconds between submitting the request and receiving the "
            "first byte of the first message",
            ["report_by", "centre_id", "topic", "protocol"],
            registry=self.registry,
        )
        self.response_invalid_format = Gauge(
            _METRIC_PREFIX + "response_invalid_format_flag",
            "Set to 1 if the response from the Global Replay service was "
            "incorrectly formatted",
            ["report_by", "centre_id", "topic", "protocol"],
            registry=self.registry,
        )
        self.response_invalid_number_matched = Gauge(
            _METRIC_PREFIX + "response_invalid_numberMatched_flag",
            "Set to 1 if the number of messages returned by the synchronous "
            "fetch did not match numberMatched (http only)",
            ["report_by", "centre_id", "topic", "protocol"],
            registry=self.registry,
        )
        self.http_response_code = Gauge(
            _METRIC_PREFIX + "http_response_code_value",
            "HTTP status code of the fetch's request (synchronous Features GET — "
            "the last page when paging — or asynchronous Processes POST); 0 = no "
            "response received",
            ["report_by", "centre_id", "topic", "protocol"],
            registry=self.registry,
        )


class _QuietRequestHandler(WSGIRequestHandler):
    """WSGI request handler that logs to the module logger instead of stderr."""

    def log_message(self, format: str, *args) -> None:  # noqa: A002
        logger.debug("metrics http: " + format, *args)


def _build_app(metrics: Metrics, endpoint: str):
    """A WSGI app that serves metrics only at ``endpoint`` and 404s elsewhere."""
    prometheus_app = make_wsgi_app(registry=metrics.registry)

    def app(environ, start_response):
        if environ.get("PATH_INFO", "/") == endpoint:
            return prometheus_app(environ, start_response)
        start_response("404 Not Found", [("Content-Type", "text/plain")])
        return [b"Not Found\n"]

    return app


def start_metrics_server(
    metrics: Metrics, endpoint: str, port: int
) -> threading.Thread:
    """Start the metrics HTTP server in a daemon thread and return that thread."""
    httpd = make_server(
        "", port, _build_app(metrics, endpoint), handler_class=_QuietRequestHandler
    )
    thread = threading.Thread(
        target=httpd.serve_forever, name="metrics-server", daemon=True
    )
    thread.start()
    logger.info("Serving Prometheus metrics on :%d%s", port, endpoint)
    return thread
