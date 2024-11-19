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
            },
            'microservice_registry': {
                'mappings': {
                    'properties': {
                        'message_type': {'type': 'keyword'},
                        'node_id': {'type': 'keyword'},
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

    def _update_registry_status(self, node_id: str, new_status: str):
        """Update the status of a node in the microservice registry"""
        try:
            # Search for the existing document
            search_result = self.es.search(
                index='microservice_registry',
                body={
                    'query': {
                        'term': {
                            'node_id': node_id
                        }
                    }
                }
            )

            if search_result['hits']['total']['value'] > 0:
                # Get the document ID
                doc_id = search_result['hits']['hits'][0]['_id']
                
                # Update the document
                self.es.update(
                    index='microservice_registry',
                    id=doc_id,
                    body={
                        'doc': {
                            'status': new_status,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                    }
                )
                self.logger.info(f"Updated status for node {node_id} to {new_status}")

        except Exception as e:
            self.logger.error(f"Error updating registry status: {str(e)}")

    def _handle_registration(self, message_value: dict):
        """Handle registration message by storing in microservice_registry"""
        try:
            # Use node_id as document ID to ensure one record per node
            self.es.index(
                index='microservice_registry',
                document={
                    'message_type': 'REGISTRATION',
                    'node_id': message_value['node_id'],
                    'service_name': message_value['service_name'],
                    'status': message_value['status'],
                    'timestamp': message_value['timestamp']
                },
                id=message_value['node_id']
            )
            self.logger.info(f"Registered node {message_value['node_id']} in registry")
            
        except Exception as e:
            self.logger.error(f"Error handling registration: {str(e)}")

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
                    message_value = message.value
                    
                    # Handle registration messages
                    if message.topic == KAFKA_CONFIG['registration_topic']:
                        self._handle_registration(message_value)
                    
                    # Handle heartbeat DOWN messages
                    elif (message.topic == KAFKA_CONFIG['heartbeat_topic'] and 
                          message_value.get('status') == 'DOWN'):
                        self._update_registry_status(message_value['node_id'], 'DOWN')
                    
                    # Get the appropriate index name and store in original index
                    index_name = self._get_index_name(message.topic)
                    if index_name:
                        response = self.es.index(
                            index=index_name,
                            document=message_value,
                            id=message_value.get('log_id') or message_value.get('node_id')
                        )
                        
                        self.logger.info(f"Indexed document in {index_name}", 
                                       document_id=response['_id'],
                                       index=index_name)
                        self.logger.info(message_value)
                    
                except Exception as e:
                    self.logger.error(f"Error processing message: {str(e)}")
                    continue
                
        except Exception as e:
            self.logger.error(f"Fatal error in consumer: {str(e)}")
            raise
        
        finally:
            self.consumer.close()
            self.es.close()