# Municipal Environmental Sensor Streaming Pipeline

This prototype implements Task 2 of the Data Engineering portfolio: a stream processing pipeline for near real-time municipal environmental sensor data.

A Python producer reads the UCI Air Quality CSV sensor readings row by row as a stream and publishes each reading to Apache Kafka. A Python consumer reads them from Kafka, flags missing measurements, stores validated events in MongoDB, and evaluates the temperature alert rule for future dashboards, reports, or citizen alert services.

## Architecture

```text
UCI Air Quality CSV
        |
        v
Python producer
        |
        v
Kafka topic: environmental-readings
        |
        v
Python consumer
        |
        v
MongoDB collection: sensor_readings
```

## Technology Stack

- Python for the producer and consumer services
- Apache Kafka for publish-subscribe event streaming
- MongoDB for flexible storage of sensor readings
- Docker Compose for reproducible local execution
- GitHub for version control and documentation

## Data Source

The main data source is the UCI Machine Learning Repository Air Quality dataset:

- Dataset page: <https://archive.ics.uci.edu/dataset/360/air+quality>
- Local file: `data/AirQualityUCI.csv`

The CSV contains hourly air quality readings from a multisensor device, including temperature, relative humidity, absolute humidity, and multiple gas sensor responses. The producer replays rows with a delay to simulate a near real-time stream.

For quick local tests, the producer streams the first 25 rows by default. Change `PRODUCER_MAX_ROWS` in `docker-compose.yml` to stream more rows, or set it to `0` to stream the whole file.

## Run The Prototype

**Before you start:** make sure Docker is running. On Mac or Windows, open Docker Desktop and wait for it to start. Then open your computer's Terminal and navigate to the folder that contains `docker-compose.yml`. Run the command there, not in Docker Desktop's **Exec** tab.

Start the complete pipeline:

```bash
docker compose up --build
```

The producer publishes UCI air-quality readings every two seconds. The consumer subscribes to the Kafka topic and stores each event in MongoDB.

Inspect stored records from another terminal:

```bash
docker compose exec mongo mongosh municipal_environment --eval 'db.sensor_readings.find().limit(5).pretty()'
```

Stop the pipeline:

```bash
docker compose down
```

### Kafka Startup Troubleshooting

If Compose reports that Kafka exited during startup, check
`docker compose logs --tail=80 kafka`. If you see `NodeExistsException` for
`/brokers/ids/1`, ZooKeeper may still hold the previous broker registration.
Leave ZooKeeper running, wait about 30 seconds, and retry from the project
folder with `docker compose up -d`. Use `docker compose ps -a` to confirm Kafka
is healthy and the consumer is running. The producer normally exits with code
0 after the replay. Do not delete volumes to recover from this error.

## Implementation Notes

Kafka separates data collection from data processing. The producer only publishes sensor events to a Kafka topic. The consumer independently subscribes to the topic and writes data to MongoDB. This allows future consumers such as an alert service or dashboard service to be added without changing the producer.

MongoDB is used because municipal sensor data may evolve over time. New measurements such as CO2, particulate matter or additional noise indicators can be added as fields in future events without redesigning a rigid relational schema.

## Temperature Alerts

The consumer stores the latest alert state per sensor and rule in the `alerts`
collection. These temperature thresholds are demonstration settings, not official
safety limits:

- Above 30 C: open an alert if none is active.
- Below 28 C: resolve an active alert.
- From 28 through 30 C: keep the existing state.
- Missing temperature: keep the existing state, not a recovery indication.
- Invalid nonnumeric or nonfinite temperature: warn and leave the alert unchanged.

The existing data-quality check reports missing measurements separately. A
`NOTIFICATION` in the consumer logs is emitted only when an alert opens or resolves;
there is no email, SMS, or push delivery. The state remains in MongoDB across
consumer restarts. Missing temperature does not prove the condition is safe.

The document ID combines the sensor ID and `high_temperature`. Each document
includes `status`, `opened_at`, `resolved_at`, thresholds, and the last processed
event and measurement time. Opening/resolution times use the source measurement
time; `updated_at` uses processing time. Replayed events at or before the last
processed measurement time do not change that alert. This simple policy ignores
late historical corrections. This is latest-state storage, not a full alert history.
The evaluator assumes the current single consumer processes each sensor in order;
notification delivery is not transactional or guaranteed exactly once.

Inspect state in Docker Desktop's mongo Exec tab by starting
`mongosh municipal_environment`, then running:

```javascript
db.alerts.find().pretty()
```

### Reproduce the Alert Test

The first 25 UCI readings do not exceed 30°C, so this test sends made-up temperatures through Kafka without changing the original CSV file. The consumer saves the test readings in MongoDB with the source label `Synthetic alert test`. For historical queries, filter for `sensor_id: "UCI-AIR-QUALITY-001"` to exclude test readings.

Open a terminal on your computer and go to the project folder containing `docker-compose.yml`. Run `docker compose up --build -d`. Once the services have started, paste the entire code block below into the **same terminal**, not into Docker Desktop's **Exec** tab. The code block prepares the `send_test_readings` command but does not send any readings yet. When the terminal prompt returns, continue to item 1 below the code block.

```bash
TEST_SENSOR="TEST-ALERT-$(docker compose exec -T consumer python -c 'from uuid import uuid4; print(uuid4().hex[:12])')"

send_test_readings() {
  docker compose exec -T consumer python - "$TEST_SENSOR" "$@" <<'PY'
import csv
import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer
from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SENSOR_DATA_FILE
from src.producer import build_uci_event

sensor_id, *temperatures = sys.argv[1:]
with open(SENSOR_DATA_FILE, newline="", encoding="utf-8") as source:
    row = next(csv.DictReader(source, delimiter=";"))

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
errors = []

def on_delivery(error, message):
    if error is not None:
        errors.append(str(error))

for value in temperatures:
    event = build_uci_event(row, 1)
    event["sensor_id"] = sensor_id
    event["event_id"] = f"{sensor_id}-{uuid4().hex[:12]}"
    event["observed_at"] = datetime.now(timezone.utc).isoformat()
    event["source"] = {"name": "Synthetic alert test"}
    event["measurements"]["temperature_c"] = None if value == "null" else float(value)
    producer.produce(KAFKA_TOPIC, key=sensor_id, value=json.dumps(event),
                     on_delivery=on_delivery)
    pending = producer.flush(10)
    if pending or errors:
        raise RuntimeError(f"Kafka delivery failed: {errors}; pending: {pending}")
    print(f"Sent {value} for {sensor_id} ({event['event_id']})")
PY
}
```

1. In the same terminal where you pasted the setup code, run:

   ```bash
   send_test_readings 25 31
   ```

   You should see two `Sent` lines with the same `TEST-ALERT-...` sensor ID. **"Sent" means Kafka received the readings; it doesn't mean the consumer has processed them yet.** To check that, run:

   ```bash
   docker compose logs consumer | grep -F "$TEST_SENSOR"
   ```

   `$TEST_SENSOR` holds the ID generated by the setup block. The command shows only log lines for your test sensor. Look for **one opening notification at 31°C** and none at 25°C. You can also find the same ID under **consumer-1 → Logs** in Docker Desktop. If the notification hasn't appeared yet, wait a moment and run the log command again.

   Then check the saved alert in MongoDB:

   ```bash
   docker compose exec -T mongo mongosh municipal_environment --quiet \
     --eval "db.alerts.findOne({sensor_id: '$TEST_SENSOR'})"
   ```

   Confirm that the alert is `active` and note its `opened_at` timestamp. You will compare it after restarting the consumer in item 2.

2. Restart only the consumer, then send a second high reading in the same terminal:

   ```bash
   docker compose restart consumer
   send_test_readings 32
   ```

   Kafka and MongoDB stay running during the restart. Repeat the log and MongoDB commands from item 1. Once MongoDB shows `last_temperature_c: 32`, confirm that the alert is still `active` and that `opened_at` hasn't changed. The logs should still show only one opening notification for this sensor.

3. Send a reading with a missing temperature, followed by a reading between the two thresholds:

   ```bash
   send_test_readings null 29
   ```

   Here, `null` means no temperature was provided. Repeat the log and MongoDB checks. Look for a missing-temperature warning, but no resolution notification. Once MongoDB shows `last_temperature_c: 29`, the alert should still be `active`.

4. Send a reading below the clear threshold:

   ```bash
   send_test_readings 27
   ```

   Repeat the checks. Look for one resolution notification in the logs and confirm that MongoDB shows `status: 'resolved'`, `last_temperature_c: 27`, and a `resolved_at` timestamp.

## Expected Phase 2 Explanation

The Phase 2 submission should explain how the Docker Compose setup starts Kafka, MongoDB, the producer, and the consumer. It should also include the GitHub repository link, the command to run the system, and a short reflection on implementation decisions and encountered issues.
