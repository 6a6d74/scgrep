I want to build a Sensor Centre to test the performance of Global Replay services in real time.  
  
Let’s call the application “SCGRep” (Sensor Centre Global Replay).  
  
Before we get started - some context:  
  
- This application will be part of the WIS2 ecosystem  
- The Sensor Centre concept is described in section “3.3.1 Overview” here: [https://wmo-im.github.io/wis2-guide/guide/infcom/guide/wis2-guide-DRAFT-INFCOM-4.html#_3_3_1_overview](https://wmo-im.github.io/wis2-guide/guide/infcom/guide/wis2-guide-DRAFT-INFCOM-4.html#_3_3_1_overview)  
- The Global Replay service is described in section “3.2.1 Global Replay” here: [https://wmo-im.github.io/wis2-guide/guide/infcom/guide/wis2-guide-DRAFT-INFCOM-4.html#_3_2_1_global_replay](https://wmo-im.github.io/wis2-guide/guide/infcom/guide/wis2-guide-DRAFT-INFCOM-4.html#_3_2_1_global_replay)  
- There will be one or more instances of a Global Replay service  
  
The application should be:  
  
- Written in Python  
- Run in a docker container; by default the container name should be “scgrep”  
- Use a Redis instance running in another docker container for keeping track of state - this is already provisioned as a service on a local docker bridge network  
- Use the paho mqtt client  
  
The application builds on a test harness that we built earlier. See the GitHub repository here: [https://github.com/6a6d74/mqtt-subscriber-web](https://github.com/6a6d74/mqtt-subscriber-web) . The mqtt-subscriber-web application may provide some insight about how to interact with the WIS2 ecosystem. The SCGRep application differs from the earlier test harness in that:  
  
- SCGRep runs as a background process all the time  
- SCGRep publishes results as prometheus metrics and does not have a Web interface  
  
The SCGRep application should not re-use any code from mqtt-subscriber-web. Let’s build a new application with a completely new GitHub repository.  
  
Please make a plan of how to build this application, then execute that plan. You should also include the necessary deployment artefacts for docker.  
  
Environment variables:  
  
- SENSOR_CENTRE_ID (no default value): Name of the sensor centre instance (example: io-wis2dev-myinstance-sensor-centre-global-replay)  
- GLOBAL_BROKER_URLS (default value = [mqtts://everyone:everyone@globalbroker.meteo.fr:8883](mqtts://everyone:everyone@globalbroker.meteo.fr:8883)): Comma-delimited list of MQTT URLs for Global Brokers (example URL: [mqtts://user:pass@host:8883](mqtts://user:pass@host:8883))   
- SUBSCRIPTION_TOPICS (no default values): Comma-delimited list of topics to validate (topics may include wildcards) - SCGRep should normally be configured to test between 10 and 20 topics; topics should be a mixture of notification (e.g., `cache/a/wis2/ca-eccc-msc/data/core/weather/surface-based-observations/synop`) and event (e.g., `monitor/a/wis2/ca-eccc-msc`) topics  
- GLOBAL_REPLAY_CENTRE_IDS (default value = `ca-eccc-msc-global-replay`): Comma-delimited list of centre-ids for the Global Replay services being tested - each CENTRE_ID relates to the corresponding value in the GLOBAL_REPLAY_URLS list, the number of values in both _URLS and _CENTRE_IDS lists must be the same  
- GLOBAL_REPLAY_URLS (default value = [https://wis2-grep.weather.gc.ca](https://wis2-grep.weather.gc.ca)): Comma-delimited list of URLs for the Global Replay services being tested - each URL relates to the corresponding value in the GLOBAL_REPLAY_CENTRE_IDS list, the number of values in both _URLS and _CENTRE_IDS lists must be the same  
- REDIS_URL (default = `redis:6379`): Redis server hostname and port  
- METRICS_ENDPOINT (default = `/metrics`): Relative URL where prometheus metrics are published  
- TIME_LAG (default = 300): Minimum time delay in seconds after being published by the Global Brokers which messages are expected to be available from the Global Replay services  
- TEST_INTERVAL (default = 300): Duration in seconds between attempts to retrieve messages from Global Replay services - each test attempt will retrieve messages that are between (TIME_LAG + TEST_INTERVAL) and TIME_LAG seconds old   
  
The SCGRep application must publish the following prometheus metrics:  
  

| Metric name | Labels | Type | Description |
| ------------------------------------------------------- | ------------------------------------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| wmo_wis2_scgrep_messages_received_during_interval_total | report_by, topic | Gauge | Total number of messages received from Global Brokers on the specified topic during the test period |
| wmo_wis2_scgrep_messages_fetched_during_interval_total | report_by, centre_id, topic, protocol | Gauge | Total number of messages retrieved from the specified Global Replay service on the specified topic during the test period* |
| wmo_wis2_scgrep_test_aborted_flag | report_by, centre_id, topic, protocol | Gauge (boolean) | Boolean set to true if the test was aborted because the time to retrieve messages from the Global Replay service exceeded the test period |
| wmo_wis2_scgrep_fetch_delay_time | report_by, centre_id, topic, protocol | Gauge | Time in milliseconds between the request to the Global Replay service being submitted and the first byte of the first message being received |
| wmo_wis2_scgrep_response_invalid_format_flag | report_by, centre_id, topic, protocol | Gauge (boolean) | Boolean set to true if the HTTP response from the OGC API end-point of the Global Replay service was incorrectly formatted. |
  
  
\* note that not all messages are actually retrieved with the HTTP fetch - only the first page of messages is retrieved which contains a count of the total number of matching messages found by the Global Replay service   
  
Labels are as follows:  
  
- `report_by`: the name of the sensor centre instance (see environment variable SENSOR_CENTRE_ID)  
- `centre_id`: the name of the Global Replay service instance being tested (see environment variable GLOBAL_REPLAY_CENTRE_IDS)  
- `topic`: the topic on which this message was originally published (see environment variable SUBSCRIPTION_TOPICS)  
- `protocol`: the protocol used to retrieve messages from the Global Replay service - either `http` or `mqtt`   
  
Here’s an overview of how the SCGRep application should work:  
  
- Upon initialisation  
    - SCGRep should generate a unique subscriber ID (a UUID) for asynchronous fetch of messages from the Global Replay services being tested  
    - Subscribe to the configured Global Brokers (see environment variable GLOBAL_BROKER_URLS) on the configured topics to test (see environment variable SUBSCRIPTION_TOPICS) and the Global Replay topics:  
        - topic = `replay/a/wis2/<centre-id>/<subscriber-id>/#`  
        - where `centre-id` is the name of the Global Replay service (see environment variable GLOBAL_REPLAY_CENTRE_IDS), and `subscriber-id` is the UUID subscriber ID generated above  
- On receipt of messages from a Global Broker, for each message:  
    - Determine the MQTT `topic` on which the message was received    
    - Extract the `id` value from the JSON message  
    - Extract either the `time` or `properties.pubtime` value from the JSON message (this will vary depending on whether it is a Notification or Event message) - for simplicity, we’ll just label this value as `time`  
    - Insert a record into Redis containing `id`, `time`, and `topic`  
    - Note that messages with the same `id` may arrive from multiple Global Brokers - this is by design; if a duplicate arrives (i.e., there is already a record in Redis with the `id` value) simply discard the newly arrived message  
    - Messages stored in Redis should automatically expire when they are no longer required for the test - expiry age should be TIME_LAG + TEST_INTERVAL + 60 seconds  
- After TEST_INTERVAL seconds has elapsed (and then every TEST_INTERVAL seconds thereafter)  
    - Execute the tests as follows  
    - Set the test period as: (current-time - TIME_LAG - TEST_INTERVAL) to (current-time - TIME_LAG)  
    - Note that the application should execute the tasks for each test period in parallel not in series (this avoids running out of time before the next test period begins), e.g.:
    
```
                                                     +-- Synchronous fetch from Global Replay 1
                                                     |
                                                     |-- Synchronous fetch from Global Replay 2
                 +-- topic 1 -- calculate baseline --+
                 |                                   |-- Asynchronous fetch from Global Replay 1
                 |                                   |
                 |                                   +-- Asynchronous fetch from Global Replay 2 
initialisation --+ 
                 |                                   +-- Synchronous fetch from Global Replay 1
                 |                                   |
                 |                                   |-- Synchronous fetch from Global Replay 2
                 +-- topic 2 -- calculate baseline --+
                                                     |-- Asynchronous fetch from Global Replay 1
                                                     |
                                                     +-- Asynchronous fetch from Global Replay 2
```

-
    - For each topic configured in SUBSCRIPTION_TOPICS  
        - Calculate the baseline:  
            - Query the Redis database to count the number of messages received from Global Brokers for this test period on the topic being evaluated  
            - Publish this count as metric `wmo_wis2_scgrep_messages_received_during_interval_total`   
        - For each Global Replay service being tested  
            - Fetch messages from the Global Replay service using a synchronous request  
                - The Global Replay service’s API end-point must conform to the OGC API - Features specification [https://docs.ogc.org/is/17-069r4/17-069r4.html](https://docs.ogc.org/is/17-069r4/17-069r4.html)  
                - The request URL will be formatted as follows: `<global-replay-url>/collections/wis2-notification-messages/items?datetime=<test-period-start-time>/<test-period-end-time>&topic=<topic>`, where  
                    - `global-replay-url` is the URL of the Global Replay service being tested  
                    - `test-period-start-time` is the time that this test period begins, expressed in ISO 8601 notation YYYY-MM-DDThh:mm:ssZ (e.g., 2026-03-19T12:20:00Z)  
                    - `test-period-end-time` is the time that this test period ends, expressed in ISO 8601 notation YYYY-MM-DDThh:mm:ssZ (e.g., 2026-03-19T12:25:00Z)  
                    - `topic` is the topic being evaluated  
                - Start a timer and submit the HTTP GET request with headers 'accept: application/json' and 'Content-Type: application/json'   
                - The Global Replay service may provide a paginated response requiring a client application to page through multiple requests to retrieve all the messages - however, each response also includes the property `numberMatched` which is the total number of messages that were found by the Global Replay service  
                - If the timer exceeds 95% of the TEST_INTERVAL before the first byte of the HTTP response is received from the Global Replay service  
                    - set metric `wmo_wis2_scgrep_test_aborted_flag` to 1 (`true`) with label `protocol` set to `http`  
                    - set metric `wmo_wis2_scgrep_messages_fetched_during_interval_total` to zero with label `protocol` set to `http`  
                    - set metric `wmo_wis2_scgrep_fetch_delay_time` to 95% of the TEST_INTERVAL (time in milliseconds) with label `protocol` set to `http`  
                    - abort the test cycle for this Global Replay service on this topic - it’s taking too long!  
                - else, when the first byte of the HTTP response is received from the Global Replay service  
                    - stop the timer and publish the value (time in milliseconds) as metric `wmo_wis2_scgrep_fetch_delay_time` with label `protocol` set to `http`  
                    - set metric `wmo_wis2_scgrep_test_aborted_flag` to 0 (`false`) with label `protocol` set to `http`  
                - parse the HTTP response and extract the `numberMatched` value; if the `numberMatched` property is found  
                    - set metric `wmo_wis2_scgrep_response_invalid_format_flag` to 0 (`false`) with label `protocol` set to `http`  
                    - set metric `wmo_wis2_scgrep_messages_fetched_during_interval_total` to the value of `numberMatched` with label `protocol` set to `http`  
                - else  
                    - set metric `wmo_wis2_scgrep_response_invalid_format_flag` to 1 (`true`) with label `protocol` set to `http`  
                    - set metric `wmo_wis2_scgrep_messages_fetched_during_interval_total` to zero with label `protocol` set to `http`  
            - Fetch messages from the Global Replay service using an asynchronous request  
                - The Global Replay service’s API end-point must conform to the OGC API - Processes - Part 1: Core specification [https://docs.ogc.org/is/18-062r2/18-062r2.html](https://docs.ogc.org/is/18-062r2/18-062r2.html)   
                - The request URL will be formatted as follows: `<global-replay-url>/processes/wis2-grep-subscriber/execution`, where  
                    - `global-replay-url` is the URL of the Global Replay service being tested  
                - Start a timer and submit the HTTP POST request with headers 'accept: application/json' and 'Content-Type: application/json’, and JSON payload as follows  
  
```json
{
  "inputs": {
    "datetime": "<test-period-start-time>/<test-period-end-time>",
    "subscriber-id": "<subscriber-id>",
    "topic": "<topic>"
  }
}
```
-
    - 
        -
            -
                - … where   
                    - `test-period-start-time` is the time that this test period begins, expressed in ISO 8601 notation YYYY-MM-DDThh:mm:ssZ (e.g., 2026-03-19T12:20:00Z)  
                    - `test-period-end-time` is the time that this test period ends, expressed in ISO 8601 notation YYYY-MM-DDThh:mm:ssZ (e.g., 2026-03-19T12:25:00Z)  
                    - `subscriber-id` is the unique UUID generated on initialisation  
                    - `topic` is the topic being evaluated  
                - The Global Replay service will publish messages on the unique MQTT topic configured for this client - to which we’re already subscribing   
                - If the timer exceeds 95% of the TEST_INTERVAL before the first byte is received from a Global Broker on topic `replay/a/wis2/<centre-id>/<subscriber-id>/<topic>` (where `centre-id` is the name of the Global Replay service being tested, `subscriber-id` is the unique UUID generated on initialisation, and `topic` is the topic being evaluated)  
                    - set metric `wmo_wis2_scgrep_test_aborted_flag` to 1 (`true`) with label `protocol` set to `mqtt`  
                    - set metric `wmo_wis2_scgrep_messages_fetched_during_interval_total` to zero with label `protocol` set to `mqtt`  
                    - set metric `wmo_wis2_scgrep_fetch_delay_time` to 95% of the TEST_INTERVAL (time in milliseconds) with label `protocol` set to `mqtt`  
                    - abort the test cycle for this Global Replay service on this topic - it’s taking too long!  
                - else  
                    - validate the HTTP ‘metadata’ JSON response   
                        - the JSON response must contain a `subscriptions` array containing one or more link objects following the [OGC API](https://ogcapi.ogc.org/) / [STAC](https://stacspec.org/) link convention  
                        - Each object describes a subscription to a** **Global Broker, with fields:  

| Field | Purpose |
| ------- | -------------------------------------------------------------------------- |
| rel | Link relation — "items" means this link points to a collection of features |
| type | Media type of the linked resource |
| href | MQTT broker URL (with embedded credentials everyone:everyone) |
| title | Human-readable name of the broker operator |
| channel | MQTT topic to subscribe to (a replay channel with a specific UUID) |

-
    -
        -
            -
                -
                    -  
                        - check that the `channel` property in each link object points to the correct topic, e.g., `replay/a/wis2/<centre-id>/<subscriber-id>/<topic>`, where   
                            - `centre-id` is the name of the Global Replay service being tested  
                            - `subscriber-id` is the unique UUID generated on initialisation  
                            - `topic` is the topic being evaluated  
                        - check that each Global Broker URL configured using the environment variable GLOBAL_BROKER_URLS is present in a `href` property of a link object in the `subscriptions` array  
                        - if either of these checks fail, or the timer exceeds 95% of the TEST_INTERVAL before the first byte of the HTTP response is received set metric `wmo_wis2_scgrep_response_invalid_format_flag` to 1 (`true`) with label `protocol` set to `mqtt`, else set the metric to 0 (`false`)  
                    - When the first byte is received from a Global Broker on topic `replay/a/wis2/<centre-id>/<subscriber-id>/<topic>` (where `centre-id` is the name of the Global Replay service being tested, `subscriber-id` is the unique UUID generated on initialisation, and `topic` is the topic being evaluated)  
                        - publish the value of the timer (time in milliseconds) as metric `wmo_wis2_scgrep_fetch_delay_time` with label `protocol` set to `mqtt`  
                        - set metric `wmo_wis2_scgrep_test_aborted_flag` to 0 (`false`) with label `protocol` set to `mqtt`  
                    - Increment a counter each time a message is received on the topic `replay/a/wis2/<centre-id>/<subscriber-id>/<topic>` (where `centre-id` is the name of the Global Replay service being tested, `subscriber-id` is the unique UUID generated on initialisation, and `topic` is the topic being evaluated)  
                    - When the timer reaches 95% of the TEST_INTERVAL publish the value of the counter as metric `wmo_wis2_scgrep_messages_fetched_during_interval_total` with label `protocol` set to `mqtt`  
  
The SCGrep application should keep running until stopped, executing the tests at the TEST_INTERVAL frequency.  
  
  
Limitations: The SCGrep application  
- does not test any filtering of the messages beyond datetime and topic.  
- does not validate the Notification or Event messages returned by the Global Replay services - this could be added in future by adding schema validation of each message as it is received, a metric `wmo_wis2_scgrep_message_invalid_format_total` could be used to count the number of messages that fail validation in a given test period.  
- does not match actual messages (based on message ID) - only counts  
- performs very limited validation of the HTTP response from OGC API - Features (only checking if the `numberMatched` property is included)   
  
Note: the Global Replay service Feature API only has a single collection (WIS2 notification messages) even though this should also include WIS2 Monitoring Event Messages too - not a problem, perhaps a little confusing.   
  
