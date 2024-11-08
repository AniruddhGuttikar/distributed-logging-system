import time
import random
import uuid
from datetime import datetime
from microservices.base_service import BaseService

class OrderService(BaseService):
    def __init__(self):
        super().__init__("OrderService")
        
    def process_order(self, order_id):
        try:
            # Simulate processing time
            processing_time = random.uniform(0.1, 2.0)
            time.sleep(processing_time)
            
            # Log based on processing time
            if processing_time > 1.5:
                log_data = {
                    "log_id": str(uuid.uuid4()),
                    "node_id": self.node_id,
                    "log_level": "WARN",
                    "message_type": "LOG",
                    "message": f"Order {order_id} processing time exceeded threshold",
                    "service_name": self.service_name,
                    "response_time_ms": int(processing_time * 1000),
                    "threshold_limit_ms": 1500,
                    "timestamp": datetime.utcnow().isoformat()
                }
            else:
                log_data = {
                    "log_id": str(uuid.uuid4()),
                    "node_id": self.node_id,
                    "log_level": "INFO",
                    "message_type": "LOG",
                    "message": f"Order {order_id} processed successfully",
                    "service_name": self.service_name,
                    "timestamp": datetime.utcnow().isoformat()
                }
            
            self.log_accumulator.send_log(log_data)
            return True
        except Exception as e:
            error_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": f"Failed to process order {order_id}",
                "service_name": self.service_name,
                "error_details": {
                    "error_code": "ORDER_PROCESSING_FAILED",
                    "error_message": str(e)
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            self.log_accumulator.send_log(error_data)
            return False