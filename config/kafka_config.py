from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'log_topic': os.getenv('LOG_TOPIC'),
    'heartbeat_topic': os.getenv('HEARTBEAT_TOPIC'),
    'registration_topic': os.getenv('REGISTRATION_TOPIC')
}