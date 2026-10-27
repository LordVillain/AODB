import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8')
)

topic_name = "events"
sensors = ["sensor_1", "sensor_2", "sensor_3"]

print("Starting event generation")
try:
    while True:
        event_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        sensor_id = random.choice(sensors)
        value = round(random.uniform(20.0, 30.0), 2)

        event = {
            "sensor_id": sensor_id,
            "temp": value,
            "timestamp": event_time
        }

        producer.send(topic_name, key=sensor_id, value=event)
        print(f"Sent: {event}")

        time.sleep(0.5)

except KeyboardInterrupt:
    print("stopped")
finally:
    producer.close()