# SCGRep — Sensor Centre Global Replay service

[![tests](https://github.com/6a6d74/scgrep/actions/workflows/tests.yml/badge.svg)](https://github.com/6a6d74/scgrep/actions/workflows/tests.yml)

SCGRep is a [WIS2](https://community.wmo.int/en/activity-areas/wis) **Sensor
Centre** that continuously measures the real-time performance of one or more
**Global Replay** services and publishes the results as Prometheus metrics.

It runs as a headless background process (no web interface). It subscribes to
Global Brokers to establish a baseline of which messages *should* be available,
then — every `TEST_INTERVAL` — asks each Global Replay service to return the
messages for a past time window and compares what comes back, over both the
synchronous (OGC API - Features) and asynchronous (OGC API - Processes + MQTT)
retrieval paths.

## How it works

1. **On start-up** SCGRep generates a unique subscriber UUID and connects to
   every configured Global Broker, subscribing to:
   - the topics under test (`SUBSCRIPTION_TOPICS`), and
   - the per-subscriber replay topics
     `replay/a/wis2/<centre-id>/<subscriber-id>/#` for each Global Replay
     service.
2. **On every broker message** it records `id`, `time`
   (`time` or `properties.pubtime`) and `topic` in Redis. Duplicate `id`s
   (which legitimately arrive from multiple brokers) are discarded; records
   expire automatically after `TIME_LAG + TEST_INTERVAL + 60` seconds.
3. **Every `TEST_INTERVAL` seconds** it runs a test cycle over the window
   `(now − TIME_LAG − TEST_INTERVAL) .. (now − TIME_LAG)`. For each topic it:
   - counts the baseline of messages received from brokers (Redis), then
   - for each Global Replay service, performs a **synchronous** fetch (OGC API -
     Features `numberMatched`) and an **asynchronous** fetch (OGC API - Processes
     execution whose results are delivered back over MQTT).

   All fetches run in parallel so a cycle completes within one `TEST_INTERVAL`;
   each fetch is capped at 95% of `TEST_INTERVAL`.

## Metrics

Exposed at `METRICS_ENDPOINT` (default `/metrics`) on `METRICS_PORT`
(default `8000`):

| Metric | Labels | Description |
| --- | --- | --- |
| `wmo_wis2_scgrep_messages_received_during_interval_total` | `report_by`, `topic` | Messages received from Global Brokers on the topic during the test period (baseline). |
| `wmo_wis2_scgrep_messages_fetched_during_interval_total` | `report_by`, `centre_id`, `topic`, `protocol` | Messages retrieved from the Global Replay service (`numberMatched` for `http`; counted messages for `mqtt`). |
| `wmo_wis2_scgrep_test_aborted_flag` | `report_by`, `centre_id`, `topic`, `protocol` | `1` if retrieval exceeded the test period. |
| `wmo_wis2_scgrep_fetch_delay_time` | `report_by`, `centre_id`, `topic`, `protocol` | Milliseconds to first byte / first message. |
| `wmo_wis2_scgrep_response_invalid_format_flag` | `report_by`, `centre_id`, `topic`, `protocol` | `1` if the response was malformed. |

`protocol` is `http` (synchronous) or `mqtt` (asynchronous).

## Configuration

All configuration is via environment variables (see `.env.example`).

| Variable | Default | Description |
| --- | --- | --- |
| `SENSOR_CENTRE_ID` | *(required)* | Name of this sensor centre instance (`report_by`). |
| `SUBSCRIPTION_TOPICS` | *(required)* | Comma-delimited topics to test (wildcards allowed); typically 10–20, mixing notification and event topics. |
| `GLOBAL_BROKER_URLS` | `mqtts://everyone:everyone@globalbroker.meteo.fr:8883` | Comma-delimited Global Broker MQTT URLs. |
| `GLOBAL_REPLAY_CENTRE_IDS` | `ca-eccc-msc-global-replay` | Comma-delimited centre-ids under test. |
| `GLOBAL_REPLAY_URLS` | `https://wis2-grep.weather.gc.ca` | Comma-delimited Global Replay URLs (same length/order as centre-ids). |
| `REDIS_URL` | `redis:6379` | Redis host:port (or a `redis://` URL). |
| `METRICS_ENDPOINT` | `/metrics` | Path where metrics are served. |
| `METRICS_PORT` | `8000` | Port for the metrics HTTP server. |
| `TIME_LAG` | `300` | Seconds after publication before messages are expected to be replayable. |
| `TEST_INTERVAL` | `300` | Seconds between test cycles. |
| `LOG_LEVEL` | `INFO` | Python log level. |

## Running

### Docker

Redis is expected to already run as a service named `redis` on the shared
`jztnet` docker bridge network (an existing/external network). The full
configuration is set inline in the `environment:` block of
`docker-compose.yml`, so no extra setup is needed to start:

```bash
docker compose up --build
```

The container is named `scgrep` by default.

Edit the values in the `environment:` block of `docker-compose.yml` for your
deployment. Those values take precedence over an optional `.env` file, which may
supply additional or overridable settings (for example `LOG_LEVEL`):

```bash
cp .env.example .env      # optional; edit as needed
docker compose up --build
```

To join a different network, change the `jztnet` entry under `networks:` (set
`name:` to your network and keep `external: true`).

### Locally (development)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
export SENSOR_CENTRE_ID=... SUBSCRIPTION_TOPICS=... REDIS_URL=localhost:6379
python -m scgrep
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest
```

## Limitations

SCGRep does **not**: test message filtering beyond datetime and topic; validate
the schema of returned Notification/Event messages; match messages by `id`
(it counts only); or deeply validate the OGC API - Features response (it only
checks for `numberMatched`).

The Global Replay Feature API exposes a single collection
(`wis2-notification-messages`) which in practice also carries WIS2 Monitoring
Event Messages.

## License

Licensed under the [Apache License 2.0](LICENSE).
