# Municipal Environmental Sensor Streaming Pipeline

This prototype implements Task 2 of the Data Engineering portfolio: a stream processing pipeline for near real-time municipal environmental sensor data.

The project replays UCI Air Quality sensor readings row by row as a stream, publishes them to Apache Kafka, consumes them with a Python service, and stores validated events in MongoDB for future dashboards, reports, or citizen alert services.

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

The CSV contains hourly air-quality readings from a multisensor device, including temperature, relative humidity, absolute humidity, and multiple gas sensor responses. The producer replays rows with a delay to simulate a near real-time stream.

For quick local tests, the producer streams the first 25 rows by default. Change `PRODUCER_MAX_ROWS` in `docker-compose.yml` to stream more rows, or set it to `0` to stream the whole file.

## Run The Prototype

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

Remove the MongoDB volume if a clean database is needed:

```bash
docker compose down -v
```

## Implementation Notes

Kafka separates data collection from data processing. The producer only publishes sensor events to a Kafka topic. The consumer independently subscribes to the topic and writes data to MongoDB. This allows future consumers, such as an alert service or dashboard service, to be added without changing the producer.

MongoDB is used because municipal sensor data may evolve over time. New measurements, such as CO2, particulate matter, or additional noise indicators, can be added as fields in future events without redesigning a rigid relational schema.

## Expected Phase 2 Explanation

The Phase 2 submission should explain how the Docker Compose setup starts Kafka, MongoDB, the producer, and the consumer. It should also include the GitHub repository link, the command to run the system, and a short reflection on implementation decisions and encountered issues.
