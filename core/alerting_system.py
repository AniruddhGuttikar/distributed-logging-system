import json
import threading
from kafka import KafkaConsumer
from config.kafka_config import KAFKA_CONFIG
from utils.logger import ServiceLogger

class AlertingSystem:
    def __init__(self):
        self.logger = ServiceLogger('AlertingSystem')
        self.consumer = KafkaConsumer(
            KAFKA_CONFIG['log_topic'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            group_id='alerting_system_group'
        )
        self.running = False
        
    def start(self):
        self.running = True
        self.alert_thread = threading.Thread(target=self._monitor_logs)
        self.alert_thread.start()
        
    def stop(self):
        self.running = False
        self.alert_thread.join()
        self.consumer.close()
        
    def _monitor_logs(self):
        while self.running:
            try:
                for message in self.consumer:
                    if not self.running:
                        break
                    
                    log_data = message.value
                    if log_data.get('log_level') in ['ERROR', 'FATAL']:
                        self._handle_critical_log(log_data)
                    elif log_data.get('log_level') == 'WARN':
                        self._handle_warning_log(log_data)
            except Exception as e:
                self.logger.error(f"Error monitoring logs: {str(e)}")
                
    def _handle_critical_log(self, log_data):
        alert_message = (
            f"🚨 CRITICAL ALERT 🚨\n"
            f"Service: {log_data.get('service_name')}\n"
            f"Level: {log_data.get('log_level')}\n"
            f"Message: {log_data.get('message')}\n"
            f"Error Details: {log_data.get('error_details', 'N/A')}"
        )
        print(alert_message)  # In real system, could send to Slack, email, etc.
        
    def _handle_warning_log(self, log_data):
        alert_message = (
            f"⚠️ WARNING ALERT ⚠️\n"
            f"Service: {log_data.get('service_name')}\n"
            f"Message: {log_data.get('message')}"
        )
        print(alert_message)  