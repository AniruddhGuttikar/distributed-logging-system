from kafka import KafkaConsumer
import json
import logging

# Set up logging
# logging.basicConfig(level=logging.DEBUG)

# Create consumer with more detailed configuration
consumer = KafkaConsumer(
    'service_logs',
    'service_registrations',
    'service_heartbeats',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',  # Add a specific group ID
    api_version=(0, 10, 1),  # Specify API version
    session_timeout_ms=6000,
    heartbeat_interval_ms=3000
)

print("Starting consumer...")
print("Subscribed to topics:", consumer.subscription())

# Verify topic partitions
partitions = consumer.assignment()
print("Assigned partitions:", partitions)

try:
    for message in consumer:
        print(f"""
        Topic: {message.topic}
        Partition: {message.partition}
        Offset: {message.offset}
        Key: {message.key}
        Value: {message.value}
        Timestamp: {message.timestamp}
        """)
except KeyboardInterrupt:
    print("Shutting down consumer...")
    consumer.close()
except Exception as e:
    print(f"An error occurred: {str(e)}")
    consumer.close()