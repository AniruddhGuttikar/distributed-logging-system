# utils/logger.py
import logging
from datetime import datetime

class ServiceLogger:
    def __init__(self, service_name):
        self.logger = logging.getLogger(service_name)
        self.logger.setLevel(logging.INFO)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        console_handler.setFormatter(formatter)
        
        # Add handler
        if not self.logger.handlers:
            self.logger.addHandler(console_handler)
    
    def format_log(self, level, message, additional_data=None):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "message": message
        }
        if additional_data:
            log_data.update(additional_data)
        return log_data
    
    def info(self, message, **kwargs):
        self.logger.info(message)
        return self.format_log("INFO", message, kwargs)
    
    def warning(self, message, **kwargs):
        self.logger.warning(message)
        return self.format_log("WARN", message, kwargs)
    
    def error(self, message, **kwargs):
        self.logger.error(message)
        return self.format_log("ERROR", message, kwargs)
    
    def fatal(self, message, **kwargs):
        self.logger.critical(message)
        return self.format_log("FATAL", message, kwargs)