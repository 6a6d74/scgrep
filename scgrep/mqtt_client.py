"""MQTT connectivity to the Global Brokers and message routing.

One paho client is created per Global Broker. Every client subscribes to the
configured test topics (whose messages are stored in Redis for baselines) and to
the per-subscriber replay wildcard topics (whose messages drive the async test
counters). Incoming messages are routed by :class:`MessageHandler`.
"""

from __future__ import annotations

import json
import logging

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

from .config import BrokerConfig, Config
from .redis_store import RedisStore
from .replay_registry import ReplayRegistry

logger = logging.getLogger(__name__)


class MessageHandler:
    """Routes broker messages to Redis (baselines) or the replay registry."""

    def __init__(self, store: RedisStore, registry: ReplayRegistry) -> None:
        self._store = store
        self._registry = registry

    def on_message(self, client, userdata, msg) -> None:
        topic = msg.topic
        if topic.startswith("replay/"):
            self._registry.handle_replay(topic)
            return

        try:
            payload = json.loads(msg.payload)
        except (ValueError, TypeError):
            logger.debug("Ignoring non-JSON message on %s", topic)
            return

        msg_id = payload.get("id")
        time_value = payload.get("time")
        if time_value is None:
            props = payload.get("properties")
            if isinstance(props, dict):
                time_value = props.get("pubtime")

        if not msg_id or not time_value:
            logger.debug("Ignoring message on %s: missing id/time", topic)
            return

        self._store.store_message(str(msg_id), str(time_value), topic)


class MqttManager:
    """Manages one paho client per configured Global Broker."""

    def __init__(self, config: Config, on_message) -> None:
        self._config = config
        self._on_message = on_message
        self._clients: list[mqtt.Client] = []

    def _subscriptions(self) -> list[str]:
        return list(self._config.subscription_topics) + (
            self._config.replay_wildcard_topics()
        )

    def _build_client(self, broker: BrokerConfig) -> mqtt.Client:
        client_id = f"scgrep-{self._config.subscriber_id}-{broker.host}"
        client = mqtt.Client(
            CallbackAPIVersion.VERSION2, client_id=client_id, clean_session=True
        )
        if broker.username is not None:
            client.username_pw_set(broker.username, broker.password)
        if broker.tls:
            client.tls_set()
        client.on_connect = self._make_on_connect(broker)
        client.on_disconnect = self._make_on_disconnect(broker)
        client.on_message = self._on_message
        return client

    def _make_on_connect(self, broker: BrokerConfig):
        subscriptions = self._subscriptions()

        def on_connect(client, userdata, flags, reason_code, properties=None):
            if reason_code != 0:
                logger.error(
                    "Failed to connect to %s: %s", broker.host, reason_code
                )
                return
            logger.info("Connected to Global Broker %s", broker.host)
            # Subscribe (again) on every (re)connect so state survives drops.
            client.subscribe([(topic, 1) for topic in subscriptions])
            logger.info(
                "Subscribed to %d topics on %s", len(subscriptions), broker.host
            )

        return on_connect

    def _make_on_disconnect(self, broker: BrokerConfig):
        def on_disconnect(client, userdata, *args):
            logger.warning("Disconnected from %s; will auto-reconnect", broker.host)

        return on_disconnect

    def start(self) -> None:
        """Connect to every broker and start their network loops."""
        for broker in self._config.brokers:
            client = self._build_client(broker)
            client.reconnect_delay_set(min_delay=1, max_delay=60)
            try:
                client.connect_async(broker.host, broker.port, keepalive=60)
            except Exception:  # noqa: BLE001 - log and continue with others
                logger.exception("Error initiating connection to %s", broker.host)
                continue
            client.loop_start()
            self._clients.append(client)

    def stop(self) -> None:
        for client in self._clients:
            try:
                client.disconnect()
                client.loop_stop()
            except Exception:  # noqa: BLE001
                logger.debug("Error stopping client", exc_info=True)
        self._clients.clear()
