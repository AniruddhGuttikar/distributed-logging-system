import json
from kafka import KafkaProducer
from config.kafka_config import KAFKA_CONFIG
from utils.logger import ServiceLogger

class LogAccumulator:
    def __init__(self, service_name):
        self.service_name = service_name
        self.logger = ServiceLogger('LogAccumulator')
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def send_log(self, log_data):
        try:
            # Add service name if not present
            if 'service_name' not in log_data:
                log_data['service_name'] = self.service_name
            
            # Send to appropriate topic based on message_type
            topic = KAFKA_CONFIG['log_topic']
            if log_data.get('message_type') == 'HEARTBEAT':
                topic = KAFKA_CONFIG['heartbeat_topic']
            elif log_data.get('message_type') == 'REGISTRATION':
                topic = KAFKA_CONFIG['registration_topic']
            
            self.producer.send(topic, log_data)
            self.producer.flush()
        except Exception as e:
            self.logger.error(f"Failed to send log: {str(e)}")
            raise