from dotenv import load_dotenv
import os

load_dotenv()

ELASTICSEARCH_CONFIG = {
    'host': os.getenv('ELASTICSEARCH_HOST'),
    'log_index': 'service_logs',
    'heartbeat_index': 'service_heartbeats',
    'registration_index': 'service_registrations'
}