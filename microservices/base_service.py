import uuid
import threading
import time
from datetime import datetime
from core.log_accumulator import LogAccumulator
from utils.logger import ServiceLogger
import random
import os
import sys

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
        self.logger.info(f"Service {self.service_name} registered.")
        
    def send_heartbeat(self, status='UP'):
        """Continuously send heartbeat messages."""
        while self.running:
            try:
                if status == 'UP':
                    # Simulate random failures
                    status = "DOWN" if random.random() < 0.1 else "UP"

                heartbeat_data = {
                    "node_id": self.node_id,
                    "message_type": "HEARTBEAT",
                    "service_name": self.service_name,
                    "status": status,
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                self.log_accumulator.send_log(heartbeat_data)

                if status == "DOWN":
                    self.logger.error(f"Service {self.service_name} is DOWN.")
                    self.stop()
                    break  # Exit the heartbeat loop on failure
                time.sleep(10)
            except Exception as e:
                self.logger.error(f"Failed to send heartbeat: {str(e)}")
                
    def start(self):
        """Start the service with registration."""
        self.running = True
        self.register_service()
        self._start_no_reg()
        
    def _start_no_reg(self):
        """Start the service without re-registering."""
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self.send_heartbeat)
        self.heartbeat_thread.daemon = True  # Ensures it doesn't block exit
        self.heartbeat_thread.start()
        
    def stop(self):
        """Stop the service and wait for the thread to terminate."""
        self.running = False
        if hasattr(self, 'heartbeat_thread') and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join()
        self.logger.info(f"Service {self.service_name} stopped.")
