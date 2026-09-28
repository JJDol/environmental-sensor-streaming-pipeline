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
    PRODUCER_MAX_ROWS,
    SENSOR_DATA_FILE,
)


def parse_number(value: str) -> float | None:
    if value is None or value == "":
        return None
    number = float(value.replace(",", "."))
    return None if number == -200 else number


def parse_uci_timestamp(date_value: str, time_value: str) -> str:
    parsed = datetime.strptime(f"{date_value} {time_value}", "%d/%m/%Y %H.%M.%S")
    return parsed.replace(tzinfo=timezone.utc).isoformat()


def build_uci_event(row: dict, sequence_number: int) -> dict:
    return {
        "event_id": f"UCI-AIR-{sequence_number}",
        "sensor_id": "UCI-AIR-QUALITY-001",
        "district": "Italian urban roadside",
        "latitude": None,
        "longitude": None,
        "observed_at": parse_uci_timestamp(row["Date"], row["Time"]),
        "published_at": datetime.now(timezone.utc).isoformat(),
        "measurements": {
            "temperature_c": parse_number(row["T"]),
            "humidity_percent": parse_number(row["RH"]),
            "absolute_humidity": parse_number(row["AH"]),
            "co_gt": parse_number(row["CO(GT)"]),
            "benzene_gt": parse_number(row["C6H6(GT)"]),
            "nox_gt": parse_number(row["NOx(GT)"]),
            "no2_gt": parse_number(row["NO2(GT)"]),
            "pt08_s1_co": parse_number(row["PT08.S1(CO)"]),
            "pt08_s2_nmhc": parse_number(row["PT08.S2(NMHC)"]),
            "pt08_s3_nox": parse_number(row["PT08.S3(NOx)"]),
            "pt08_s4_no2": parse_number(row["PT08.S4(NO2)"]),
            "pt08_s5_o3": parse_number(row["PT08.S5(O3)"]),
        },
        "source": {
            "name": "UCI Air Quality Dataset",
            "url": "https://archive.ics.uci.edu/dataset/360/air+quality",
        },
    }


def build_event(row: dict, sequence_number: int) -> dict:
    if "Date" in row and "Time" in row:
        return build_uci_event(row, sequence_number)

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
    is_uci_dataset = data_path.name.lower() == "airqualityuci.csv"

    print(f"Publishing sensor readings from {data_path} to topic {KAFKA_TOPIC}")

    with data_path.open(newline="", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file, delimiter=";" if is_uci_dataset else ",")
        for sequence_number, row in enumerate(reader, start=1):
            if is_uci_dataset and not row.get("Date"):
                continue
            if PRODUCER_MAX_ROWS and sequence_number > PRODUCER_MAX_ROWS:
                break

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
