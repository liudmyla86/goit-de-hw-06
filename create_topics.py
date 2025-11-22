from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

TOPICS = [
    "building_sensors_liudmyla",   # input topic for sensor data
    "alerts_liudmyla"              # output topic for alerts
]

def create_topics():
    admin = AdminClient({
        "bootstrap.servers": kafka_config["bootstrap_servers"][0],
        "security.protocol": kafka_config["security_protocol"],
        "sasl.mechanism": kafka_config["sasl_mechanism"],
        "sasl.username": kafka_config["username"],
        "sasl.password": kafka_config["password"],
    })

    new_topics = [
        NewTopic(topic, num_partitions=1, replication_factor=1)
        for topic in TOPICS
    ]

    print("Creating topics...")
    fs = admin.create_topics(new_topics)

    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic '{topic}' created successfully")
        except Exception as e:
            print(f"Topic '{topic}' already exists or failed: {e}")

if __name__ == "__main__":
    create_topics()
