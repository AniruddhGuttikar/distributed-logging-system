# run_monitoring.py
from threading import Thread
from core.log_storage import ElasticsearchConsumer
from core.alerting_system import AlertSystem
from utils.logger import ServiceLogger

def run_monitoring():
    logger = ServiceLogger("MonitoringSystem")
    
    # Initialize both systems
    es_consumer = ElasticsearchConsumer()
    
    # Create threads
    es_thread = Thread(target=es_consumer.start_consuming)
    
    # Make threads daemon so they'll stop when main program stops
    es_thread.daemon = True
    
    logger.info("Starting monitoring systems...")
    
    try:
        # Start both threads
        es_thread.start()
        
        # # Keep the main thread alive
        while True:
            es_thread.join(1)  # Check every second
            
    except KeyboardInterrupt:
        logger.info("Stopping monitoring systems...")
        
    finally:
        logger.info("Monitoring systems stopped")

if __name__ == "__main__":
    run_monitoring()