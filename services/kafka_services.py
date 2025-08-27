# File: services/kafka_service.py
import json
import logging
from typing import List, Dict, Any, Optional, Callable
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import os
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class KafkaService:
    """Enhanced Kafka service with multi-topic support and robust error handling"""
    
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BROKER', 'localhost:9092')
        self.topic_raw = os.getenv('KAFKA_RAW_TOPIC')
        self.topic_clean = os.getenv('KAFKA_CLEAN_TOPIC')
        
        # Producer configuration
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all',
            'retries': 5,
            'retry.backoff.ms': 300,
            'linger.ms': 10,
            'batch.size': 32768,
            'compression.type': 'snappy',
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 1,
        }
        
        # Consumer configuration base
        self.consumer_config_base = {
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,
        }
        
        self.producer = None
        self.admin_client = None
    
    def setup_topics(self) -> bool:
        """Create topics if they don't exist"""
        try:
            admin_config = {'bootstrap.servers': self.bootstrap_servers}
            self.admin_client = AdminClient(admin_config)
            
            # Define topics
            topics = [
                NewTopic(
                    topic=self.topic_raw,
                    num_partitions=3,
                    replication_factor=1,
                    config={'cleanup.policy': 'delete', 'retention.ms': '604800000'}  # 7 days
                ),
                NewTopic(
                    topic=self.topic_clean,
                    num_partitions=3,
                    replication_factor=1,
                    config={'cleanup.policy': 'delete', 'retention.ms': '2592000000'}  # 30 days
                )
            ]
            
            # Create topics
            futures = self.admin_client.create_topics(topics)
            
            for topic, future in futures.items():
                try:
                    future.result()  # Block until topic creation completes
                    logger.info(f"Topic '{topic}' created successfully")
                except KafkaException as e:
                    if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                        logger.info(f"Topic '{topic}' already exists")
                    else:
                        logger.error(f"Failed to create topic '{topic}': {e}")
                        return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error setting up topics: {e}")
            return False
    
    def get_producer(self) -> Producer:
        """Get or create Kafka producer"""
        if self.producer is None:
            self.producer = Producer(self.producer_config)
        return self.producer
    
    def delivery_report(self, err, msg):
        """Delivery report callback for producer"""
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def produce_message(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce a single message to specified topic"""
        try:
            producer = self.get_producer()
            
            # Serialize message
            message_bytes = json.dumps(message, ensure_ascii=False, default=str).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            
            producer.produce(
                topic=topic,
                key=key_bytes,
                value=message_bytes,
                callback=self.delivery_report
            )
            
            producer.poll(0)  # Trigger delivery callbacks
            return True
            
        except Exception as e:
            logger.error(f"Error producing message to {topic}: {e}")
            return False
    
    def produce_to_raw(self, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce message to raw topic"""
        return self.produce_message(self.topic_raw, message, key)
    
    def produce_to_clean(self, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """Produce message to clean topic"""
        return self.produce_message(self.topic_clean, message, key)
    
    def produce_batch(self, topic: str, messages: List[Dict[str, Any]]) -> int:
        """Produce batch of messages to specified topic"""
        success_count = 0
        producer = self.get_producer()
        
        try:
            for message in messages:
                key = message.get('id') or message.get('link')
                if self.produce_message(topic, message, key):
                    success_count += 1
            
            # Flush all messages
            producer.flush(timeout=30)
            logger.info(f"Produced {success_count}/{len(messages)} messages to {topic}")
            
        except Exception as e:
            logger.error(f"Error in batch produce to {topic}: {e}")
        
        return success_count
    
    def get_consumer(self, group_id: str, topics: List[str]) -> Consumer:
        """Create consumer for specified topics"""
        consumer_config = self.consumer_config_base.copy()
        consumer_config['group.id'] = group_id
        
        consumer = Consumer(consumer_config)
        consumer.subscribe(topics)
        
        logger.info(f"Consumer created for topics: {topics}, group: {group_id}")
        return consumer
    
    def consume_messages(self, consumer: Consumer, 
                        message_handler: Callable[[Dict[str, Any]], bool],
                        timeout: float = 1.0,
                        max_messages: int = 100) -> int:
        """Consume and process messages"""
        processed_count = 0
        
        try:
            while processed_count < max_messages:
                msg = consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug(f"End of partition reached {msg.topic()} [{msg.partition()}]")
                        continue
                    else:
                        logger.error(f"Consumer error: {msg.error()}")
                        break
                
                try:
                    # Deserialize message
                    message_data = json.loads(msg.value().decode('utf-8'))
                    
                    result = message_handler(message_data)
                    if result is None or result is True:
                        processed_count += 1
                        logger.debug(f"Processed message from {msg.topic()} [{msg.partition()}] offset {msg.offset()}")
                    else:
                        logger.warning(f"Failed to process message from {msg.topic()}")

                        
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        
        return processed_count
    
    def health_check(self) -> Dict[str, bool]:
        """Check Kafka cluster health"""
        health = {
            'producer': False,
            'topics_exist': False,
            'admin_client': False
        }
        
        try:
            # Test producer
            producer = self.get_producer()
            test_message = {'type': 'health_check', 'timestamp': str(os.times())}
            health['producer'] = self.produce_message(self.topic_raw, test_message)
            
            # Test admin client
            if self.admin_client is None:
                admin_config = {'bootstrap.servers': self.bootstrap_servers}
                self.admin_client = AdminClient(admin_config)
            
            # Check if topics exist
            metadata = self.admin_client.list_topics(timeout=10)
            existing_topics = set(metadata.topics.keys())
            required_topics = {self.topic_raw, self.topic_clean}
            
            health['topics_exist'] = required_topics.issubset(existing_topics)
            health['admin_client'] = True
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
        
        return health
    
    def close(self):
        """Close all connections"""
        if self.producer:
            self.producer.flush(timeout=10)
            self.producer = None
        
        logger.info("Kafka service closed")


