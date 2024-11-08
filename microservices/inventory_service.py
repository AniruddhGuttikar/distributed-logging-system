import time
import random
import uuid
from datetime import datetime
from microservices.base_service import BaseService

class InventoryService(BaseService):
    def __init__(self):
        super().__init__("InventoryService")
        
    def update_inventory(self, item_id, quantity):
        try:
            # Simulate inventory update processing time
            processing_time = random.uniform(0.1, 1.5)
            time.sleep(processing_time)
            
            # Simulate random out-of-stock scenario
            if random.random() < 0.1:  # 10% chance of failure
                raise Exception("Item out of stock")
            
            # Log successful inventory update
            log_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": f"Inventory updated for item {item_id}, quantity adjusted by {quantity}",
                "service_name": self.service_name,
                "timestamp": datetime.utcnow().isoformat()
            }
            self.log_accumulator.send_log(log_data)
            return True
            
        except Exception as e:
            # Log failure in inventory update
            error_data = {
                "log_id": str(uuid.uuid4()),
                "node_id": self.node_id,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": f"Failed to update inventory for item {item_id}",
                "service_name": self.service_name,
                "error_details": {
                    "error_code": "INVENTORY_UPDATE_FAILED",
                    "error_message": str(e)
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            self.log_accumulator.send_log(error_data)
            return False
