import uuid
import threading
import time
from datetime import datetime
from core.log_accumulator import LogAccumulator
from utils.logger import ServiceLogger
import random

class BaseService:
    def __init__(self, service_name):
        self.node_id = str(uuid.uuid4())
        self.service_name = service_name
        self.log_accumulator = LogAccumulator(service_name)
        self.logger = ServiceLogger(service_name)
        self.running = False
        
    def register_service(self):
        registration_data = {
            "node_id": self.node_id,
            "message_type": "REGISTRATION",
            "service_name": self.service_name,
            "status": "UP",
            "timestamp": datetime.utcnow().isoformat()
        }
        self.log_accumulator.send_log(registration_data)
        
    def send_heartbeat(self):
        while self.running:
            try:
                status = "DOWN" if random.random() < 0.1 else "UP"

                heartbeat_data = {
                    "node_id": self.node_id,
                    "message_type": "HEARTBEAT",
                    "service_name": self.service_name,
                    "status": status,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                self.log_accumulator.send_log(heartbeat_data)
                
                # If status is DOWN, stop the service
                if status == "DOWN":
                    self.logger.error(f"Service {self.service_name} is DOWN. Stopping service.")
                    self.stop()
                    break  # Exit the loop after stopping the service
                time.sleep(10)  # Heartbeat interval
            except Exception as e:
                self.logger.error(f"Failed to send heartbeat: {str(e)}")
                
    def start(self):
        self.running = True
        self.register_service()
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        
    def stop(self):
        self.running = False
        if hasattr(self, 'heartbeat_thread'):
            self.heartbeat_thread.join()