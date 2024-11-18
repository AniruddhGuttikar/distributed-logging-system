from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'service_logs', 'service_registrations', 'service_heartbeats',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)


print("Subscribed to topics:", consumer.subscription())
for message in consumer:
    print(f"Received message from {message.topic}: {message.value}")
