# SCGRep — Sensor Centre Global Replay service

[![tests](https://github.com/6a6d74/wis2-scgrep/actions/workflows/tests.yml/badge.svg)](https://github.com/6a6d74/wis2-scgrep/actions/workflows/tests.yml)

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

> For a component-by-component description of the codebase and the Docker stack,
> see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

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
reachable over HTTPS at `https://<host>/metrics` (see [Running](#running)):

| Metric | Labels | Description |
| --- | --- | --- |
| `wmo_wis2_scgrep_messages_received_during_interval_total` | `report_by`, `topic` | **Cumulative counter**, incremented each test period by the messages received from Global Brokers on the topic (the baseline). Use `increase(...[60s])` for a per-interval value. |
| `wmo_wis2_scgrep_messages_fetched_during_interval_total` | `report_by`, `centre_id`, `topic`, `protocol` | **Cumulative counter**, incremented each test period by the messages retrieved from the Global Replay service — `numberMatched` for `http`; for `mqtt`, a Redis-deduplicated count of the replayed messages (the same message delivered by several replay brokers is counted once). Use `increase(...[60s])` for a per-interval value. |
| `wmo_wis2_scgrep_test_aborted_flag` | `report_by`, `centre_id`, `topic`, `protocol` | `1` if retrieval did not complete within the test period. For `mqtt` this means the replay reported messages (`numberMatched > 0`) but delivered none over MQTT before the deadline; a `numberMatched = 0` window does **not** abort. |
| `wmo_wis2_scgrep_fetch_delay_time` | `report_by`, `centre_id`, `topic`, `protocol` | Timeliness in milliseconds: `http` = time to first byte; `mqtt` = time to the first replayed message, or — when no messages are expected (`numberMatched = 0`) — the time to the process-execution HTTP response. |
| `wmo_wis2_scgrep_response_invalid_format_flag` | `report_by`, `centre_id`, `topic`, `protocol` | `1` if the response was malformed. |
| `wmo_wis2_scgrep_response_invalid_numberMatched_flag` | `report_by`, `centre_id`, `topic`, `protocol` | **Synchronous (`http`) fetch only:** `1` if the number of messages actually returned (across all pages) did not equal `numberMatched`. |

`protocol` is `http` (synchronous) or `mqtt` (asynchronous).

The two `messages_*_during_interval_total` metrics are **cumulative counters**:
they are *incremented* by each test period's count and never reset, so they only
grow (until the process restarts). This leaves how they are aggregated up to the
query — take `increase(...[60s])` to recover the per-interval count, or sum/rate
over any longer window. The remaining metrics are point-in-time gauges. Prometheus
scrapes every 15s (see `prometheus/prometheus.yml`) so a 60s `increase()` window
always spans several samples.

All metrics for a given test cycle are updated **together**, at ~95% of the test
period (when the asynchronous fetch completes): the counters are incremented and
the gauges set at that same instant, so a single Prometheus scrape sees a
consistent set of values for the cycle rather than a mix of old and new.

### Why the baseline and fetch counts differ

For a given topic and window the three counts — the baseline
(`messages_received`), the synchronous fetch (`messages_fetched`, `http`), and
the asynchronous fetch (`messages_fetched`, `mqtt`) — measure *the same thing by
three different routes*, so some difference between them is normal. The point of
the tool is to watch **how large** the difference is.

**Small differences are expected and not a fault.** They arise because:

- The three counts come from **different mechanisms** — messages the Sensor
  Centre captured live from the Global Brokers (baseline), a point-in-time
  `numberMatched` query against the Global Replay store (`http`), and the
  replayed messages delivered over MQTT and recorded in Redis (`mqtt`, counted
  the same way as the baseline and deduplicated by message `id` across replay
  brokers) — sampled at **slightly different instants** against a continuously
  updating stream.
- Messages near the **edges of the datetime window** can fall on either side
  depending on exactly when each query runs, shifting a few messages in or out.
- The baseline reflects only what **this subscriber** actually received (subject
  to broker relay lag, deduplication, and connection timing), while the Global
  Replay store may hold marginally more or fewer for the same window.

**Larger or persistent differences are the signal to investigate:**

- **`http` (or `mqtt`) well below the baseline** — the Global Replay service is
  returning fewer messages than were seen on the brokers, i.e. it is missing data
  for that window/topic. This is exactly the performance gap SCGRep exists to
  surface.
- **`http` above the baseline** — the replay store holds messages this subscriber
  did not receive (e.g. it missed them, or the store includes messages from
  brokers the subscriber is not connected to).
- **`mqtt` at zero with `test_aborted_flag` set** — the replay reported messages
  for the window (`numberMatched > 0`) but delivered none over the broker SCGRep
  subscribes to within the deadline. This is a genuine async-delivery/timeliness
  failure, or a configuration/coverage issue — e.g. during the preoperational
  phase the replay service may publish replays to its own broker rather than the
  operational Global Brokers (see `GLOBAL_REPLAY_BROKER_URLS`). When the replay
  genuinely has nothing for the window (`numberMatched = 0`), `mqtt` is zero but
  the test does **not** abort — that gap surfaces as a baseline-vs-fetched
  difference instead.
- **`response_invalid_format_flag` set** — the response was malformed, so its
  count is unreliable regardless of the number.

The Grafana **Differences** panel (below) plots `baseline − fetched` directly, so
these gaps are easy to spot at a glance: a line hovering near zero is healthy;
a line trending away from zero is a service that is losing or gaining messages.

### Investigating a discrepancy

When a topic shows a large baseline-vs-fetched gap for a window, confirm whether
it is a genuine Global Replay gap (messages the service is missing) before
concluding anything. Work with the window's **datetime interval** — SCGRep counts
by message `pubtime`, and the Global Replay `datetime` filter does too.

1. **Confirm the baseline is real** from the log **file** (not `docker logs`, see
   the gotchas below). The per-cycle `Baseline:` line reports the count and the
   interval; the received-message lines let you count directly:

   ```bash
   # baseline the app logged for that cycle
   grep 'Baseline: topic=cache/a/wis2/us-noaa-nws/#' logs/scgrep.log \
     | grep 'interval=2026-08-16T12:08:52Z/2026-08-16T12:09:52Z'

   # messages actually received with pubtime in the window
   grep 'Global Broker message: topic=cache/a/wis2/us-noaa-nws' logs/scgrep.log \
     | grep -oE 'time=2026-08-16T[0-9:]+' | sed 's/time=2026-08-16T//' \
     | awk -F: '{t=$1":"$2":"$3} t>="12:08:52" && t<="12:09:52"' | wc -l
   ```

2. **Ask the Global Replay directly** — URL-encode the `#` as `%23` (a browser
   treats a raw `#` as a fragment and silently drops the wildcard, so it queries
   the wrong topic and returns 0):

   ```bash
   curl -s "https://wis2-grep.weather.gc.ca/collections/wis2-notification-messages/items\
   ?datetime=2026-08-16T12:08:52Z/2026-08-16T12:09:52Z&topic=cache/a/wis2/us-noaa-nws/%23" \
     | python3 -c 'import sys,json; print("numberMatched:", json.load(sys.stdin)["numberMatched"])'
   ```

3. **Map it minute by minute** to see whether the replay has a real hole. Compare
   the replay's `numberMatched` against the received count for each one-minute
   window around the gap (widen the window first — a service with data at
   `12:00–12:20` but `0` at `12:09–12:10` has genuinely lost that minute):

   ```bash
   for m in $(seq 4 12); do
     s=$(printf '2026-08-16T12:%02d:00Z' $m); e=$(printf '2026-08-16T12:%02d:00Z' $((m+1)))
     n=$(curl -s "https://wis2-grep.weather.gc.ca/collections/wis2-notification-messages/items?datetime=$s/$e&topic=cache/a/wis2/us-noaa-nws/%23" \
         | python3 -c 'import sys,json; print(json.load(sys.stdin)["numberMatched"])')
     echo "$s .. $e -> replay=$n"
   done
   ```

   Minutes that match the baseline confirm the pipeline is sound; a minute where
   the replay is well below the baseline (or `0`) is a genuine gap to raise with
   the Global Replay operators.

4. **Or let the log do it for you.** `scripts/replay_loss_report.py` performs the
   same minute-by-minute comparison directly from the SCGRep log files — no live
   queries needed. For a topic it tabulates, per one-minute pub-time window, the
   replayed-message count, the baseline count, and their difference, then draws an
   ASCII histogram of the difference. Both counts come from the per-message log
   lines (`Global Broker message:` and `Replay message (…):`), de-duplicated by
   `id`; note that per-message replay logging only exists from when that feature
   was deployed, so windows older than that show `0` replayed.

   ```bash
   # last hour for a topic, from ./logs/scgrep.log (see -h for all options)
   python scripts/replay_loss_report.py -t us-noaa-nws

   # a specific window, synchronous replay only
   python scripts/replay_loss_report.py -t us-noaa-nws -s sync \
     --since 2026-08-16T13:30:00Z --until 2026-08-16T14:00:00Z
   ```

   To line up with the **Grafana metrics** instead of clock minutes, add
   `-s summary`: it reads the per-cycle summary lines (`Test period begins:` /
   `Result:`) and reports over the **exact tested windows** — the same values
   SCGRep publishes to Prometheus — with both `http` (`numberMatched`) and `mqtt`
   counts and their differences from the baseline:

   ```bash
   python scripts/replay_loss_report.py -t us-noaa-nws -s summary
   ```

   Note that only the **values** line up — the **times** do not. A window shown in
   the report appears roughly **6–7 minutes later** on the Grafana dashboard
   (`TIME_LAG` + `TEST_INTERVAL` + the Prometheus scrape interval + Grafana's ~15s
   rounding), and the report is in **UTC** while Grafana usually renders local
   time. See the script's `-h` for the full breakdown.

   A large positive difference for a window is the same signal as above: messages
   the Sensor Centre saw live that the replay service did not return.

**Gotchas that make a real result look like a bug:**

- **`docker logs scgrep` only shows the *current* container.** After any
  `docker compose up`/restart the stream resets, so older windows appear empty.
  The complete history is in `logs/scgrep.log`.
- **A raw `#` in a browser URL is a fragment**, not the MQTT wildcard — always
  encode it as `%23` (curl and the app already do).

**How a genuine gap shows up across the metrics.** For a window the replay is
missing (baseline non-zero, but the replay has the messages neither via `http`
nor `mqtt`), expect all of the following together — they corroborate one gap, not
several faults:

- `messages_received` (baseline) **> 0**, `messages_fetched{http}` and
  `messages_fetched{mqtt}` **= 0** (large `Differences` for both protocols);
- `response_invalid_format_flag{http} = 0` — the `http` fetch got a clean
  `numberMatched=0` response (nothing malformed);
- `test_aborted_flag{mqtt} = 0` — the async fetch decides whether replays are due
  from the replay's **own** `numberMatched` (from the synchronous fetch), not
  from the baseline. A `numberMatched=0` gap means *nothing is due over MQTT*, so
  the async fetch does **not** abort; `fetch_delay_time{mqtt}` reports the HTTP
  (process-execution) response delay instead of the abort value.

`test_aborted_flag{mqtt} = 1` is reserved for the **opposite** case: the replay
reported `numberMatched > 0` but failed to deliver those messages over MQTT
within the deadline — a genuine async-delivery problem, distinct from a data gap
(which the baseline-vs-fetched `Differences` already surface). If `numberMatched`
is unavailable (the synchronous fetch itself failed), the async fetch falls back
to the baseline for this decision.

## Configuration

All configuration is via environment variables (see `.env.example`).

| Variable | Default | Description |
| --- | --- | --- |
| `SENSOR_CENTRE_ID` | *(required)* | Name of this sensor centre instance (`report_by`). |
| `SUBSCRIPTION_TOPICS` | *(required)* | Comma-delimited topics to test (wildcards allowed); typically 10–20, mixing notification and event topics. |
| `GLOBAL_BROKER_URLS` | `mqtts://everyone:everyone@globalbroker.meteo.fr:8883` | Comma-delimited Global Broker MQTT URLs. |
| `GLOBAL_REPLAY_CENTRE_IDS` | `ca-eccc-msc-global-replay` | Comma-delimited centre-ids under test. |
| `GLOBAL_REPLAY_URLS` | `https://wis2-grep.weather.gc.ca` | Comma-delimited Global Replay URLs (same length/order as centre-ids). |
| `GLOBAL_REPLAY_BROKER_URLS` | `mqtts://everyone:everyone@wis2-grep.weather.gc.ca:8883` | Comma-delimited broker(s) on which async replay messages are available (one during the preoperational phase). The operational Global Brokers do not yet republish this GRep's messages; the WIS2 test Global Broker (`gb.wis2dev.io:8883`, used in the Compose deployment) does, and the GRep instance's own broker (the default) also works. **Leave blank** (`""`) to receive replays via the Global Brokers in `GLOBAL_BROKER_URLS` themselves — the operational case, where no separate replay broker is needed. Only supports a **single** GRep instance. |
| `GLOBAL_REPLAY_BROKER_TLS_INSECURE` | `false` | Skip TLS verification for the replay broker. Set `true` only if its certificate lapses. |
| `REDIS_URL` | `redis:6379` | Redis host:port (or a `redis://` URL). |
| `METRICS_ENDPOINT` | `/metrics` | Path where metrics are served. |
| `METRICS_PORT` | `8000` | Port for the metrics HTTP server. |
| `TIME_LAG` | `300` | Seconds after publication before messages are expected to be replayable. |
| `TEST_INTERVAL` | `300` | Seconds between test cycles. |
| `REDIS_STARTUP_TIMEOUT` | `60` | Seconds to wait for Redis to become reachable on startup before giving up. |
| `LOG_LEVEL` | `INFO` | Python log level. The detailed activity logs are at `INFO`; set `WARNING` to keep only warnings/errors. |
| `LOG_HTTP_RESPONSE_MAX_CHARS` | `1000` | Max characters of an HTTP response body written to the log (longer bodies are truncated). |
| `LOG_FILE` | *(unset)* | Also write logs to this file (in addition to stdout). Unset/blank = stdout only. |
| `LOG_FILE_FLUSH_INTERVAL` | `60` | How often (seconds) buffered logs are written to `LOG_FILE`. |

## Logging

Logs go to stdout, each line timestamped (`docker compose logs -f scgrep` to
follow). The detailed activity below is logged at `INFO` — **on by default** —
so it is always captured unless explicitly turned off with `LOG_LEVEL=WARNING`
(which keeps only the warnings/errors):

- **Broker connections** — each Global Broker and replay broker connection, and
  the exact topics subscribed on it.
- **Test period start/finish** — the window each period covers.
- **Unique broker messages** — every newly-seen message from the Global Brokers
  (`topic`, `id`, `time`); duplicates that are discarded are not logged.
- **Replay messages** — one line per message returned by a Global Replay service
  (`centre_id`, `topic`, `id`, `time`), tagged `Replay message (asynchronous)`
  for MQTT-delivered messages (duplicates from other replay brokers are not
  logged) and `Replay message (synchronous)` for every Feature in a synchronous
  response.
- **Requests** to a Global Replay service — `centre_id`, type (synchronous /
  asynchronous), topic, datetime interval, and the full request (the POST payload
  for asynchronous requests; each page of a synchronous response is a separate
  logged request; HTTP headers are not logged).
- **Responses** — `centre_id`, type, topic, and the response body truncated at
  `LOG_HTTP_RESPONSE_MAX_CHARS`.
- **`numberMatched`** extracted from each synchronous response's first page.
- **Per-result summary** — one line per (service, topic): baseline, fetched,
  fetch delay, and the aborted / invalid-format / invalid-numberMatched flags.

Emitted at `WARNING`/`ERROR` (always shown): **aborted tests** (synchronous
timeout or no replay within the deadline), **malformed / invalid responses**,
failed process executions, broker disconnects, and a **`numberMatched` mismatch**
(the synchronous fetch returned a different number of messages than
`numberMatched`).

Timestamps are **UTC**.

### Writing logs to a file

Set `LOG_FILE` to also write logs to a file (in addition to stdout). Records are
buffered and written every `LOG_FILE_FLUSH_INTERVAL` seconds (default 60) —
so the file updates in batches, not live (use stdout for a live tail); errors are
written immediately. A `WatchedFileHandler` reopens the file if it is replaced,
so the file can be trimmed underneath a running app.

In the Compose deployment this is enabled and points at
`/var/log/scgrep/scgrep.log`, mounted to `./logs/scgrep.log` on the host.

### Purging old log entries

`scripts/purge_logs.py` removes log entries older than a cutoff from a log file,
in place (it rewrites the file atomically, keeping only entries at/after the
cutoff; entries are matched by their leading UTC timestamp):

```bash
python scripts/purge_logs.py ./logs/scgrep.log --max-age-hours 24
```

The Compose stack runs this **hourly** as a small `log-purge` service (it reuses
the SCGRep image for Python, runs the script from the mounted `./scripts`, and
keeps `PURGE_LOG_MAX_AGE_HOURS` hours — default 24). To run it on a schedule
outside Docker instead, point cron at the same script, e.g.:

```cron
0 * * * * /usr/bin/python3 /path/to/scripts/purge_logs.py /path/to/logs/scgrep.log --max-age-hours 24
```

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

- Metrics: `https://localhost/metrics` (Traefik `websecure` entrypoint on `:443`;
  plain HTTP on `:80` is redirected to HTTPS)
- Traefik dashboard: `https://localhost/dashboard/`, protected by HTTP basic auth.

#### TLS certificate

TLS is terminated by Traefik. For a **warning-free local HTTPS**, generate a
locally-trusted certificate with [`mkcert`](https://github.com/FiloSottile/mkcert):

```bash
mkcert -install   # one-time: adds a local CA to your system trust store (needs your password)
mkcert -cert-file traefik/certs/localhost.pem -key-file traefik/certs/localhost-key.pem \
       localhost 127.0.0.1 ::1
docker compose up -d traefik   # reload
```

Traefik then serves that certificate (see the `tls:` block in
`traefik/dynamic.yml`), and `https://localhost` is trusted by any machine on
which `mkcert -install` has been run — no `curl -k` and no browser warning. The
certificate files live in `traefik/certs/` and are **git-ignored** (they are
machine-specific; each user generates their own).

If `traefik/certs/` is empty, Traefik falls back to its built-in **self-signed**
certificate — HTTPS still works, but clients must skip verification (`curl -k`,
or `insecure_skip_verify` in a Prometheus scrape). Note that `localhost` can
never have a *publicly*-trusted certificate; for a real deployment, use a proper
DNS name with an ACME/Let's Encrypt resolver.

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

The Compose stack already includes **Prometheus** and **Grafana**, pre-wired:

- Prometheus: `http://localhost:9090` — scrapes `scgrep:8000` (config in
  `prometheus/prometheus.yml`).
- Grafana: `http://localhost:3000` (default login `admin` / `admin` — change it)
  — the Prometheus datasource **and** a pre-built SCGRep dashboard are
  auto-provisioned from `grafana/provisioning/` (see [Dashboard](#dashboard)).

Data is persisted in the `prometheus-data` and `grafana-data` volumes.

### Dashboard

The provisioned dashboard **SCGRep - Global Replay performance**
(`http://localhost:3000/d/scgrep-overview`) gives an at-a-glance view of one
Global Replay service across the tested topics.

**Selectors** (top of the dashboard, applied to every panel):

- **Global Replay service** — the `centre_id` label, **single-select** (pick one
  service to inspect).
- **Topic(s)** — the `topic` label, **multi-select** with an **All** option; the
  list is scoped to the selected service.

**Panels** — six stacked time-series panels sharing one time axis and a shared
crosshair, so a hover lines up across all of them:

| # | Panel | Series | Reads as |
| --- | --- | --- | --- |
| 1 | **Totals** | `increase(...[60s])` of baseline, `http`, `mqtt` counters | how many messages each route saw per interval |
| 2 | **Differences** | `increase[60s]` of `baseline − http`, `baseline − mqtt` | near zero = healthy; drifting away = the service is losing/gaining messages |
| 3 | **Differences (%)** | panel 2 as a percentage of the baseline | 0% = perfect match; positive % = the service returned fewer than the baseline (undefined when the baseline is zero) |
| 4 | **Timeliness** | `http` and `mqtt` fetch delay (ms) | how quickly the service responds / first replay arrives |
| 5 | **Test status** | `http` and `mqtt` aborted flag (0/1) | 1 = retrieval did not complete in time (`mqtt`: `numberMatched > 0` but nothing delivered over MQTT) |
| 6 | **Format validation** | `http` and `mqtt` invalid-format flag (0/1) | 1 = a malformed response |
| 7 | **Synchronous fetch validation** | `http` invalid-numberMatched flag (0/1) | 1 = the synchronous fetch returned a different message count than `numberMatched` |

Because the baseline (`messages_received`) has no `centre_id` label, panels 1–3
filter it by topic only; the `http`/`mqtt` series filter by both service and
topic. See [Why the baseline and fetch counts differ](#why-the-baseline-and-fetch-counts-differ)
for how to interpret panels 1 and 2.

The rest of this section is for pointing an **external** Prometheus at SCGRep.

### 1. Add a scrape job

Edit your `prometheus.yml` and add an entry under `scrape_configs`. Choose the
target based on where Prometheus runs relative to the SCGRep stack:

```yaml
scrape_configs:
  # (a) Prometheus runs on the same Compose network ("traefik"): scrape the
  #     scgrep container directly by service name (plain HTTP backend, no TLS).
  - job_name: scgrep
    # Metrics change once per test cycle (TEST_INTERVAL, default 300s), so a
    # scrape interval at or below that is plenty.
    scrape_interval: 60s
    metrics_path: /metrics
    static_configs:
      - targets: ["scgrep:8000"]

  # (b) Prometheus runs elsewhere and reaches SCGRep through Traefik over HTTPS.
  #     Traefik uses a self-signed certificate, so verification is skipped.
  #     Use the host/IP where the stack is published (host.docker.internal from
  #     another container on Docker Desktop, or the host's LAN address).
  # - job_name: scgrep-via-traefik
  #   scheme: https
  #   tls_config:
  #     insecure_skip_verify: true
  #   scrape_interval: 60s
  #   metrics_path: /metrics
  #   static_configs:
  #     - targets: ["host.docker.internal:443"]
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

SCGRep does **not**:

- test message filtering beyond datetime and topic;
- validate the schema of the returned Notification/Event messages;
- reconcile the actual message **ids** across the baseline, synchronous and
  asynchronous results — it records the ids of every message in Redis and
  compares *counts*, but does not yet match the id sets against one another.

The `messages_fetched` metric for the synchronous (`http`) fetch reports
`numberMatched`. SCGRep now additionally pages through the whole result set,
counts the messages actually returned, records each in Redis, and flags any
mismatch with `numberMatched` via
`wmo_wis2_scgrep_response_invalid_numberMatched_flag`.

The Global Replay Feature API exposes a single collection
(`wis2-notification-messages`) which in practice also carries WIS2 Monitoring
Event Messages.

## License

Licensed under the [Apache License 2.0](LICENSE).
