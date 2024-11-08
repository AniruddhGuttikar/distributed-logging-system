import json
import threading
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from config.kafka_config import KAFKA_CONFIG
from utils.logger import ServiceLogger

class HeartbeatMonitor:
    def __init__(self):
        self.logger = ServiceLogger('HeartbeatMonitor')
        self.services = {}  # Store service status
        self.consumer = KafkaConsumer(
            KAFKA_CONFIG['heartbeat_topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='heartbeat_monitor_group'
        )
        self.running = False
        
    def start(self):
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_heartbeats)
        self.check_thread = threading.Thread(target=self._check_service_health)
        self.monitor_thread.start()
        self.check_thread.start()
        
    def stop(self):
        self.running = False
        self.monitor_thread.join()
        self.check_thread.join()
        self.consumer.close()
        
    def _monitor_heartbeats(self):
        while self.running:
            try:
                for message in self.consumer:
                    if not self.running:
                        break
                    
                    heartbeat = message.value
                    self.services[heartbeat['node_id']] = {
                        'last_heartbeat': datetime.utcnow(),
                        'service_name': heartbeat.get('service_name', 'Unknown'),
                        'status': heartbeat.get('status', 'UP')
                    }
            except Exception as e:
                self.logger.error(f"Error monitoring heartbeats: {str(e)}")
                
    def _check_service_health(self):
        while self.running:
            try:
                current_time = datetime.utcnow()
                for node_id, info in self.services.items():
                    last_heartbeat = info['last_heartbeat']
                    if current_time - last_heartbeat > timedelta(seconds=30):  # 30 seconds timeout
                        self.logger.warning(
                            f"Service {info['service_name']} (Node: {node_id}) might be down. "
                            f"Last heartbeat: {last_heartbeat.isoformat()}"
                        )
                        info['status'] = 'DOWN'
                time.sleep(10)  # Check every 10 seconds
            except Exception as e:
                self.logger.error(f"Error checking service health: {str(e)}")