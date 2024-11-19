# elasticsearch_consumer.py
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
from datetime import datetime
from config.elasticsearch_config import ELASTICSEARCH_CONFIG
from config.kafka_config import KAFKA_CONFIG
from utils.logger import ServiceLogger

class ElasticsearchConsumer:
    def __init__(self):
        self.logger = ServiceLogger("ElasticsearchConsumer")
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='elasticsearch_consumer_group'
        )
        
        # Subscribe to all three topics
        self.consumer.subscribe([
            KAFKA_CONFIG['log_topic'],
            KAFKA_CONFIG['heartbeat_topic'],
            KAFKA_CONFIG['registration_topic']
        ])
        
        # Initialize Elasticsearch client
        self.es = Elasticsearch([ELASTICSEARCH_CONFIG['host']])
        
        # Create indices if they don't exist
        self._create_indices()
    
    def _create_indices(self):
        """Create Elasticsearch indices with proper mappings if they don't exist"""
        indices = {
            ELASTICSEARCH_CONFIG['log_index']: {
                'mappings': {
                    'properties': {
                        'log_id': {'type': 'keyword'},
                        'node_id': {'type': 'keyword'},
                        'log_level': {'type': 'keyword'},
                        'message_type': {'type': 'keyword'},
                        'message': {'type': 'text'},
                        'service_name': {'type': 'keyword'},
                        'timestamp': {'type': 'date'}
                    }
                }
            },
            ELASTICSEARCH_CONFIG['heartbeat_index']: {
                'mappings': {
                    'properties': {
                        'node_id': {'type': 'keyword'},
                        'message_type': {'type': 'keyword'},
                        'service_name': {'type': 'keyword'},
                        'status': {'type': 'keyword'},
                        'timestamp': {'type': 'date'}
                    }
                }
            },
            ELASTICSEARCH_CONFIG['registration_index']: {
                'mappings': {
                    'properties': {
                        'node_id': {'type': 'keyword'},
                        'message_type': {'type': 'keyword'},
                        'service_name': {'type': 'keyword'},
                        'status': {'type': 'keyword'},
                        'timestamp': {'type': 'date'}
                    }
                }
            }
        }
        
        for index_name, mapping in indices.items():
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(index=index_name, body=mapping)
                self.logger.info(f"Created index: {index_name}")
    
    def _get_index_name(self, topic: str) -> str:
        """Map Kafka topic to Elasticsearch index name"""
        topic_to_index = {
            KAFKA_CONFIG['log_topic']: ELASTICSEARCH_CONFIG['log_index'],
            KAFKA_CONFIG['heartbeat_topic']: ELASTICSEARCH_CONFIG['heartbeat_index'],
            KAFKA_CONFIG['registration_topic']: ELASTICSEARCH_CONFIG['registration_index']
        }
        return topic_to_index.get(topic)
    
    def start_consuming(self):
        """Start consuming messages from Kafka and indexing them in Elasticsearch"""
        try:
            self.logger.info("Started consuming messages...")
            for message in self.consumer:
                try:
                    # Get the appropriate index name based on topic
                    self.logger.info(f"subscribed to message {message.topic}")
                    # print("received message")
                    index_name = self._get_index_name(message.topic)
                    if not index_name:
                        self.logger.error(f"Unknown topic: {message.topic}")
                        continue
                    
                    # Index the document
                    response = self.es.index(
                        index=index_name,
                        document=message.value,
                        id=message.value.get('log_id') or message.value.get('node_id')
                    )
                    
                    self.logger.info(f"Indexed document in {index_name}", 
                                   document_id=response['_id'],
                                   index=index_name)
                    print(json.dumps(message.value, indent=4))
                
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                    continue
                
        except Exception as e:
            self.logger.error(f"Fatal error in consumer: {str(e)}")
            raise
        
        finally:
            self.consumer.close()
            self.es.close()
