import json
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient, ASCENDING

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MONGO_COLLECTION,
    MONGO_DATABASE,
    MONGO_URI,
)


REQUIRED_MEASUREMENTS = {
    "temperature_c",
    "humidity_percent",
    "air_quality_index",
    "noise_db",
    "smoke_level",
}


def validate_event(event: dict) -> None:
    required_fields = {
        "event_id",
        "sensor_id",
        "district",
        "latitude",
        "longitude",
        "observed_at",
        "published_at",
        "measurements",
    }
    missing_fields = required_fields - set(event)
    if missing_fields:
        raise ValueError(f"Missing event fields: {sorted(missing_fields)}")

    missing_measurements = REQUIRED_MEASUREMENTS - set(event["measurements"])
    if missing_measurements:
        raise ValueError(f"Missing measurements: {sorted(missing_measurements)}")


def main() -> None:
    mongo_client = MongoClient(MONGO_URI)
    collection = mongo_client[MONGO_DATABASE][MONGO_COLLECTION]
    collection.create_index([("event_id", ASCENDING)], unique=True)
    collection.create_index([("sensor_id", ASCENDING), ("observed_at", ASCENDING)])

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "mongodb-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Consuming topic {KAFKA_TOPIC} and writing to MongoDB collection {MONGO_COLLECTION}")

    while True:
        message = consumer.poll(timeout=1.0)
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())

        event = json.loads(message.value().decode("utf-8"))
        try:
            validate_event(event)
            event["stored_at"] = datetime.now(timezone.utc).isoformat()
            collection.update_one(
                {"event_id": event["event_id"]},
                {"$set": event},
                upsert=True,
            )
            print(f"Stored {event['event_id']} from {event['sensor_id']}")
        except Exception as exc:
            print(f"Rejected message at offset {message.offset()}: {exc}")


if __name__ == "__main__":
    main()
