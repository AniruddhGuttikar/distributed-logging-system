import time
import random
import uuid
from datetime import datetime
from microservices.base_service import BaseService

class PaymentService(BaseService):
    def __init__(self):
        super().__init__("PaymentService")
        
    def process_payment(self, payment_id, amount):
        try:
            # Simulate payment processing
            processing_time = random.uniform(0.1, 1.5)
            time.sleep(processing_time)
            
            # Simulate random payment failure
            if random.random() < 0.1:  # 10% chance of failure
                raise Exception("Payment gateway timeout")
            
            log_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": f"Payment {payment_id} processed successfully for amount {amount}",
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
                "message": f"Failed to process payment {payment_id}",
                "service_name": self.service_name,
                "error_details": {
                    "error_code": "PAYMENT_PROCESSING_FAILED",
                    "error_message": str(e)
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            self.log_accumulator.send_log(error_data)
            return False