import csv
import json
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_INTERVAL_SECONDS,
    SENSOR_DATA_FILE,
)


def build_event(row: dict, sequence_number: int) -> dict:
    return {
        "event_id": f"{row['sensor_id']}-{sequence_number}",
        "sensor_id": row["sensor_id"],
        "district": row["district"],
        "latitude": float(row["latitude"]),
        "longitude": float(row["longitude"]),
        "observed_at": row["observed_at"],
        "published_at": datetime.now(timezone.utc).isoformat(),
        "measurements": {
            "temperature_c": float(row["temperature_c"]),
            "humidity_percent": float(row["humidity_percent"]),
            "air_quality_index": int(row["air_quality_index"]),
            "noise_db": float(row["noise_db"]),
            "smoke_level": float(row["smoke_level"]),
        },
    }


def main() -> None:
    data_path = Path(SENSOR_DATA_FILE)
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})

    print(f"Publishing sensor readings from {data_path} to topic {KAFKA_TOPIC}")

    with data_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for sequence_number, row in enumerate(reader, start=1):
            event = build_event(row, sequence_number)
            producer.produce(
                KAFKA_TOPIC,
                key=event["sensor_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            producer.flush()
            print(f"Published {event['event_id']}: {event['measurements']}")
            time.sleep(PRODUCER_INTERVAL_SECONDS)

    print("All sample readings were published.")


if __name__ == "__main__":
    main()
