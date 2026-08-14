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

Served by SCGRep at `METRICS_ENDPOINT` (default `/metrics`) on `METRICS_PORT`
(default `8000`). In the Docker deployment this is fronted by Traefik and
reachable at `http://<host>/metrics` (see [Running](#running)):

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
| `REDIS_STARTUP_TIMEOUT` | `60` | Seconds to wait for Redis to become reachable on startup before giving up. |
| `LOG_LEVEL` | `INFO` | Python log level. |

## Running

### Docker

The stack is self-contained: `docker compose up --build` starts a managed
**Redis**, the **SCGRep** service, and a **Traefik** reverse proxy, all on a
single Compose-managed bridge network named `traefik`. SCGRep waits for Redis to
be healthy (via `depends_on`) before starting, and its configuration is set
inline in the `environment:` block of `docker-compose.yml`:

```bash
cp traefik/dashboard-users.example traefik/dashboard-users   # required (see below)
docker compose up --build
```

The container is named `scgrep` by default.

Traefik fronts the metrics endpoint; SCGRep does not publish its port directly.
Traefik reaches it by service name (`scgrep:8000`) over the shared network, using
the file-provider routing in `traefik/dynamic.yml`. After `docker compose up`:

- Metrics: `http://localhost/metrics` (Traefik `web` entrypoint on `:80`)
- Traefik dashboard: `http://localhost/dashboard/`, protected by HTTP basic auth.

Before starting, create the (gitignored) credentials file from the example:

```bash
cp traefik/dashboard-users.example traefik/dashboard-users   # default admin / changeme
# then set a real credential (recommended):
htpasswd -nB admin > traefik/dashboard-users
# or, without apache2-utils:
openssl passwd -apr1 'your-password' | sed 's/^/admin:/' > traefik/dashboard-users
```

`traefik/dashboard-users` is git-ignored so credentials never land in the repo.

Edit the values in the `environment:` block of `docker-compose.yml` for your
deployment. Those values take precedence over an optional `.env` file, which may
supply additional or overridable settings (for example `LOG_LEVEL`):

```bash
cp .env.example .env      # optional; edit as needed
docker compose up --build
```

If you change `METRICS_PORT`, update the backend URL in `traefik/dynamic.yml` to
match.

### Locally (development)

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
export SENSOR_CENTRE_ID=... SUBSCRIPTION_TOPICS=... REDIS_URL=localhost:6379
python -m scgrep
```

## Scraping with Prometheus

SCGRep exposes a standard Prometheus text endpoint, so any Prometheus instance
can scrape it. Add a scrape job pointing at the metrics endpoint.

### 1. Add a scrape job

Edit your `prometheus.yml` and add an entry under `scrape_configs`. Choose the
target based on where Prometheus runs relative to the SCGRep stack:

```yaml
scrape_configs:
  # (a) Prometheus runs on the same Compose network ("traefik"): scrape the
  #     scgrep container directly by service name.
  - job_name: scgrep
    # Metrics change once per test cycle (TEST_INTERVAL, default 300s), so a
    # scrape interval at or below that is plenty.
    scrape_interval: 60s
    metrics_path: /metrics
    static_configs:
      - targets: ["scgrep:8000"]

  # (b) Prometheus runs elsewhere and reaches SCGRep through Traefik on :80.
  #     Use the host/IP where the stack is published (host.docker.internal from
  #     another container on Docker Desktop, or the host's LAN address).
  # - job_name: scgrep-via-traefik
  #   scrape_interval: 60s
  #   metrics_path: /metrics
  #   static_configs:
  #     - targets: ["host.docker.internal:80"]
```

If you run Prometheus as its own container and want it to use option (a),
attach it to the same network so it can resolve `scgrep`:

```yaml
# in your Prometheus compose file
services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    ports:
      - "9090:9090"
    networks:
      - traefik
networks:
  traefik:
    name: traefik
    external: true   # created by the SCGRep stack
```

Reload Prometheus after editing (`docker restart <prometheus>`, or `POST` to
`/-/reload` if `--web.enable-lifecycle` is set).

### 2. Confirm the target is up

Open the Prometheus UI at `http://<prometheus-host>:9090` and go to
**Status → Targets** (newer builds: **Status → Target health**). The `scgrep`
job should be listed with **State = UP** and a recent "Last Scrape". If it shows
`DOWN`, the error column explains why (DNS, connection refused, wrong path).

### 3. Explore the metrics

Go to the **Graph** page (the expression browser):

1. Click in the expression field and type `wmo_wis2_scgrep` — autocomplete lists
   all five SCGRep metrics.
2. Enter a metric name and press **Execute**. The **Table** tab shows the current
   value per label set; the **Graph** tab plots it over time.
3. Narrow down with label matchers, for example:
   - `wmo_wis2_scgrep_fetch_delay_time{protocol="mqtt"}` — async first-message
     latency, in milliseconds.
   - `wmo_wis2_scgrep_fetch_delay_time{protocol="http"}` — synchronous
     time-to-first-byte.
   - `wmo_wis2_scgrep_test_aborted_flag == 1` — only the (topic, service,
     protocol) combinations whose fetch timed out.
   - `wmo_wis2_scgrep_messages_fetched_during_interval_total` vs
     `wmo_wis2_scgrep_messages_received_during_interval_total` — replayed count
     versus the broker baseline for a topic.

These are **gauges** reporting the value for the most recent test cycle, so on
the Graph tab you see one point per cycle. A few useful expressions:

```promql
# Replayed count minus the broker baseline per topic/service (negative means the
# replay service returned fewer messages than were seen on the brokers).
wmo_wis2_scgrep_messages_fetched_during_interval_total{protocol="http"}
  - on(report_by, topic) group_left
    wmo_wis2_scgrep_messages_received_during_interval_total

# Max async fetch delay across all topics for each Global Replay service.
max by (centre_id) (wmo_wis2_scgrep_fetch_delay_time{protocol="mqtt"})

# Count of aborted tests in the latest cycle.
sum(wmo_wis2_scgrep_test_aborted_flag)
```

For dashboards and alerting, point Grafana at the same Prometheus data source, or
add Prometheus alert rules on `wmo_wis2_scgrep_test_aborted_flag` /
`wmo_wis2_scgrep_response_invalid_format_flag`.

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
