import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from configs import kafka_config

TOPIC_BUILDING = "building_sensors_liudmyla"

def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    return producer

def main():
    sensor_id = random.randint(1000, 9999)
    print(f"starting sensor with ID: {sensor_id}")

    producer = create_producer()

    try:
        while True:
            temperature = random.uniform(25, 45) #25-45
            humidity = random.uniform(15, 85) # 15-85
            message = {
                "sensor_id": sensor_id,
                "timestamp": datetime.utcnow().isoformat(),
                "temperature": round(temperature, 2),
                "humidity": round(humidity, 2),
            }

            producer.send(TOPIC_BUILDING, value=message)
            producer.flush()
            print("Sent:", message)
            time.sleep(2) # 1 time per 2 seconds
    except KeyboardInterrupt:
        print("Stopping sensor...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
