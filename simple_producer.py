from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send a test message
test_message = {
    "node_id": "test-node",
    "message_type": "TEST",
    "service_name": "TestService",
    "status": "UP",
    "timestamp": time.strftime('%Y-%m-%dT%H:%M:%S')
}

producer.send('service_logs', value=test_message)
producer.flush()
print("Test message sent")