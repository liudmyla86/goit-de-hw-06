from confluent_kafka import Consumer
from configs import kafka_config
import json

conf = {
    'bootstrap.servers': kafka_config['bootstrap_servers'][0],
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': kafka_config['username'],
    'sasl.password': kafka_config['password'],
    'group.id': 'alerts_consumer_liudmyla',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(["alerts_liudmyla"])

print(" Listening for alerts...")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Error:", msg.error())
        continue

    data = json.loads(msg.value().decode("utf-8"))
    print("\n--- ALERT RECEIVED ---")
    print(json.dumps(data, indent=4))
