"""Small shared helpers: time conversion, topic matching, URL normalisation."""

from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

from paho.mqtt.client import topic_matches_sub

__all__ = ["parse_time_to_epoch", "epoch_to_iso", "topic_matches", "broker_authority"]


def parse_time_to_epoch(value: str) -> float:
    """Parse an ISO 8601 timestamp into a UTC epoch (seconds).

    Accepts a trailing ``Z`` as well as explicit offsets. A naive timestamp is
    assumed to be UTC. Raises ``ValueError`` on unparseable input.
    """
    text = value.strip()
    if text.endswith("Z") or text.endswith("z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def epoch_to_iso(epoch: float) -> str:
    """Format a UTC epoch as ``YYYY-MM-DDThh:mm:ssZ`` (whole seconds)."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def topic_matches(subscription: str, topic: str) -> bool:
    """True if an MQTT ``topic`` matches a ``subscription`` filter (``+``/``#``)."""
    return topic_matches_sub(subscription, topic)


def topic_to_query(topic: str) -> str:
    """Convert an MQTT topic filter into the form a Global Replay service expects.

    Topics are configured (and used for MQTT subscriptions) in MQTT form, e.g.
    ``cache/a/wis2/uk-metoffice/#``. The Global Replay ``topic`` parameter is not
    an MQTT filter: it matches whole topic *levels* as a prefix, so a trailing
    ``/#`` or ``/`` matches nothing and must be removed.

    Stripping the trailing ``/#`` is what the service itself does for asynchronous
    requests; the synchronous (OGC API - Features) request has no such handling,
    so the client must do it for both. ``#`` alone becomes an empty string (the
    service has no "match everything" form).

    >>> topic_to_query("cache/a/wis2/uk-metoffice/#")
    'cache/a/wis2/uk-metoffice'
    >>> topic_to_query("cache/a/wis2/uk-metoffice/")
    'cache/a/wis2/uk-metoffice'
    """
    query = topic.strip()
    if query == "#":
        return ""
    if query.endswith("/#"):
        query = query[:-2]
    return query.rstrip("/")


def broker_authority(url: str) -> str:
    """Return ``host:port`` for an MQTT URL, ignoring credentials and scheme.

    Used to compare configured broker URLs against ``href`` values returned by
    a Global Replay service without being tripped up by credentials or default
    ports.
    """
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if parsed.port is not None:
        port = parsed.port
    else:
        port = 8883 if parsed.scheme == "mqtts" else 1883
    return f"{host}:{port}"
