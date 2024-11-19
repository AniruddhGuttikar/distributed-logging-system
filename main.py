import threading
from microservices.inventory_service import InventoryService
from microservices.order_service import OrderService
from microservices.payment_service import PaymentService
import time
import random
import uuid

def run_service(service_instance):
    """Starts and runs a service in a loop."""
    try:
        service_instance.start()
        while service_instance.running:
            if isinstance(service_instance, InventoryService):
                random_quantity = random.randint(1, 1000)
                inventory_id = uuid.uuid4()
                service_instance.update_inventory(inventory_id, random_quantity)
            elif isinstance(service_instance, OrderService):
                order_id = uuid.uuid4()
                service_instance.process_order(order_id)
            elif isinstance(service_instance, PaymentService):
                transaction_id = uuid.uuid4()
                random_quantity = random.randint(1, 1000)
                service_instance.process_payment(transaction_id, random_quantity)
            time.sleep(2)
    finally:
        service_instance.stop()


if __name__ == "__main__":
    # Initialize service instances
    inventory_service = InventoryService()
    order_service = OrderService()
    payment_service = PaymentService()

    # Create threads for each service
    services = [inventory_service, order_service, payment_service]
    threads = [threading.Thread(target=run_service, args=(service,)) for service in services]

    # Start all service threads
    for thread in threads:
        thread.start()

    # Wait for all threads to complete (in real-world use cases, this might involve signal handling)
    for thread in threads:
        thread.join()
