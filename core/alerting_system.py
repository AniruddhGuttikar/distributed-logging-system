from kafka import KafkaConsumer
import json
from datetime import datetime
from config.kafka_config import KAFKA_CONFIG
from utils.logger import ServiceLogger

class AlertSystem:
    def __init__(self):
        self.logger = ServiceLogger("AlertSystem")
        
        # Initialize Kafka consumer for logs and heartbeats
        self.consumer = KafkaConsumer(
            KAFKA_CONFIG['log_topic'],
            KAFKA_CONFIG['heartbeat_topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='alert_system_group'
        )
        
        # Track service status
        self.service_status = {}
    
    def _process_error_log(self, message):
        """Process ERROR level logs"""
        if message.get('log_level') == 'ERROR' and message.get('message_type') == 'LOG':
            alert_msg = (
                f"⚠️ ERROR LOG ALERT\n"
                f"Service: {message.get('service_name')}\n"
                f"Message: {message.get('message')}\n"
                f"Node ID: {message.get('node_id')}\n"
                f"Timestamp: {message.get('timestamp')}"
            )
            self.logger.error(alert_msg, 
                            service_name=message.get('service_name'),
                            node_id=message.get('node_id'))
    
    def _process_heartbeat(self, message):
        """Process heartbeat messages and detect DOWN status"""
        service_name = message.get('service_name')
        node_id = message.get('node_id')
        status = message.get('status')
        
        # Update service status
        key = f"{service_name}:{node_id}"
        prev_status = self.service_status.get(key)
        self.service_status[key] = status
        
        # Alert on status change to DOWN
        if status == 'DOWN' and prev_status != 'DOWN':
            alert_msg = (
                f"🔴 SERVICE DOWN ALERT\n"
                f"Service: {service_name}\n"
                f"Node ID: {node_id}\n"
                f"Status: {status}\n"
                f"Timestamp: {message.get('timestamp')}"
            )
            self.logger.error(alert_msg,
                            service_name=service_name,
                            node_id=node_id,
                            status=status)
        
        # Log recovery
        elif status == 'UP' and prev_status == 'DOWN':
            recovery_msg = (
                f"🟢 SERVICE RECOVERED\n"
                f"Service: {service_name}\n"
                f"Node ID: {node_id}\n"
                f"Status: {status}\n"
                f"Timestamp: {message.get('timestamp')}"
            )
            self.logger.info(recovery_msg,
                           service_name=service_name,
                           node_id=node_id,
                           status=status)
    
    def start_monitoring(self):
        """Start monitoring for alerts"""
        try:
            self.logger.info("Alert system started monitoring...")
            for message in self.consumer:
                try:
                    data = message.value
                    
                    if message.topic == KAFKA_CONFIG['log_topic']:
                        self._process_error_log(data)
                    elif message.topic == KAFKA_CONFIG['heartbeat_topic']:
                        self._process_heartbeat(data)
                        
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Fatal error in alert system: {str(e)}")
            raise
            
        finally:
            self.consumer.close()

