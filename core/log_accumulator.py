import json
from config.kafka_config import KAFKA_CONFIG
from utils.logger import ServiceLogger
from fluent import sender
from dotenv import load_dotenv
import os


load_dotenv()

class LogAccumulator:
    def __init__(self, service_name):
        self.service_name = service_name
        self.logger = ServiceLogger('LogAccumulator')

        self.fluent_logger = sender.FluentSender(
            tag='service',
            host=os.getenv('FLUENT_HOST', "localhost"),
            port=int(os.getenv('FLUENT_PORT', 24224))
        )
    
    def send_log(self, log_data):
        try:
            self.fluent_logger.tag="service." + log_data['message_type'].lower() + "s"            # Add service name if not present
            if 'service_name' not in log_data:
                log_data['service_name'] = self.service_name
            # print(self.fluent_logger.tag)
            # if log_data['message_type'] == "LOG":
            #     if (log_data['log_level'] == "INFO"):
            #         self.logger.info(log_data)
            #     if (log_data['log_level'] == "WARN"):
            #         self.logger.warning(log_data)
            #     if (log_data['log_level'] == "ERROR"):
            #         self.logger.error(log_data)
            print(json.dumps(log_data, indent=4))
            
            self.fluent_logger.emit(None, log_data)
        except Exception as e:
            self.logger.error(f"Failed to send log: {str(e)}")
            raise