# Phase 2 Explanation Draft

This prototype implements the stream processing pipeline planned in the conception phase. The system replays environmental sensor readings from a CSV file and publishes them as near real-time events to an Apache Kafka topic named `environmental-readings`. A separate Python consumer subscribes to this topic, validates each event, and stores the readings in a MongoDB collection for later dashboard, alerting, or analytical use.

The implementation uses Docker Compose to make the setup reproducible. The Compose file starts Zookeeper, Kafka, MongoDB, the producer service, and the consumer service. This reflects the publish-subscribe architecture discussed in the course material: the producer is responsible only for publishing sensor events, Kafka buffers the stream, and the consumer handles downstream processing and storage. This separation keeps data collection independent from data processing and makes it possible to add future consumers, such as an alert service, without changing the producer.

MongoDB was selected because sensor data may evolve when future sensors measure additional environmental values. The current event structure stores sensor ID, location, timestamp, district, and measurements such as temperature, humidity, air quality index, noise, and smoke level.

To run the prototype, the repository can be cloned and started with `docker compose up --build`. The GitHub repository link will be added after the project is uploaded.
