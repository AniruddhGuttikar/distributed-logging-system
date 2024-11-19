import sys
import threading
import time
import random
import uuid
from microservices.inventory_service import InventoryService
from microservices.order_service import OrderService
from microservices.payment_service import PaymentService

def run_service(service_instance):
    """Starts and runs a service in a loop with restart capability."""
    while True:
        try:
            # Start the service
            service_instance.start()
            print(f"Started {service_instance.service_name}")
            
            while service_instance.running:
                # Process service-specific tasks
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

                # Wait a bit before the next iteration to simulate task intervals
                time.sleep(2)
        
        except KeyboardInterrupt:
            print(f"\nStopping {service_instance.service_name}...")
            service_instance.send_heartbeat('DOWN')
            service_instance.stop()
        
        # Service stopped; prompt for user action
        print("\n" + "="*50)
        print(f"⚠️  SERVICE STOPPED: {service_instance.service_name}")
        print("\nTo restart the service:")
        print("1. Investigate the error")
        print("2. Ensure dependencies are available")
        print("3. Type 'restart' to bring the service back up")
        print("4. Type 'exit' to quit")
        print("="*50 + "\n")
        
        while True:
            command = input("Command > ").strip().lower()
            if command == 'restart':
                print(f"Restarting {service_instance.service_name}...")
                service_instance._start_no_reg()
                break
            elif command == 'exit':
                print(f"Exiting {service_instance.service_name}...")
                sys.exit(0)
            else:
                print("Invalid command. Type 'restart' or 'exit'.")

        
def print_usage():
    print(""" 
Usage: python3 run_service.py 
Available services: 
- inventory 
- order 
- payment 
- all (runs all services) 

Example: python3 run_service.py inventory 
""")

def main():
    if len(sys.argv) != 2:
        print_usage()
        sys.exit(1)
    
    service_name = sys.argv[1].lower()
    
    # Dictionary mapping service names to their classes
    service_map = { 
        'inventory': InventoryService, 
        'order': OrderService, 
        'payment': PaymentService 
    }
    
    try:
        if service_name == 'all':
            # Run all services in separate threads
            services = [ServiceClass() for ServiceClass in service_map.values()]
            threads = [threading.Thread(target=run_service, args=(service,)) for service in services]
            
            # Start all threads
            for thread in threads:
                thread.start()
            
            # Wait for all threads to complete
            try:
                for thread in threads:
                    thread.join()
            except KeyboardInterrupt:
                print("\nStopping all services...")
                for service in services:
                    service.stop()
        
        elif service_name in service_map:
            # Run single service with restart capability
            service_instance = service_map[service_name]()
            run_service(service_instance)
        
        else:
            print(f"Error: Unknown service '{service_name}'")
            print_usage()
            sys.exit(1)
    
    except KeyboardInterrupt:
        print("\nShutdown requested...")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        print("Shutdown complete.")

if __name__ == "__main__":
    main()