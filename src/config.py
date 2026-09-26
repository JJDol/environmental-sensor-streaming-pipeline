import os


KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "environmental-readings")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "municipal_environment")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "sensor_readings")
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "2"))
SENSOR_DATA_FILE = os.getenv(
    "SENSOR_DATA_FILE", "data/sample_environmental_readings.csv"
)
