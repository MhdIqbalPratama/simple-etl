import sys
import os
import signal
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.kafka_services import KafkaService
from services.staging_pg import PostgresService
from services.es import ElasticsearchService
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('clean_consumer')

class CleanConsumer:
    """Consumer: topic_clean -> upserts pg_silver + indexes to Elasticsearch"""
    
    def __init__(self):
        self.kafka_service = KafkaService()
        self.pg_service = PostgresService()
        self.es_service = ElasticsearchService()
        self.consumer = None
        self.running = False
        
        # Batch processing
        self.batch_size = int(os.getenv('BATCH_SIZE', '20'))
        self.message_batch = []
        
        # Processing statistics
        self.stats = {
            'messages_consumed': 0,
            'silver_upserted': 0,
            'es_indexed': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
    
    def setup(self) -> bool:
        """Initialize services"""
        try:
            # Setup Kafka topics
            if not self.kafka_service.setup_topics():
                logger.error("Failed to setup Kafka topics")
                return False
            
            # Setup PostgreSQL
            if not self.pg_service.connect():
                logger.error("Failed to connect to PostgreSQL")
                return False
            
            if not self.pg_service.setup_database():
                logger.error("Failed to setup database")
                return False
            
            # Setup Elasticsearch
            if not self.es_service.setup_index():
                logger.error("Failed to setup Elasticsearch index")
                return False
            
            # Create consumer for topic_clean
            group_id = os.getenv('KAFKA_GROUP_CLEAN', 'clean_consumer_group')
            self.consumer = self.kafka_service.get_consumer(
                group_id=group_id,
                topics=[self.kafka_service.topic_clean]
            )
            
            logger.info(f"✅ Clean consumer initialized - Group: {group_id}, Batch: {self.batch_size}")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False
    
    def process_message(self, message: dict) -> bool:
        """Add message to batch for processing"""
        try:
            # Validate message
            if not message.get('link') or not message.get('content'):
                logger.warning("Message missing required fields")
                return False
            
            self.message_batch.append(message)
            self.stats['messages_consumed'] += 1
            
            # Process batch when it reaches size limit
            if len(self.message_batch) >= self.batch_size:
                self.process_batch()
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding message to batch: {e}")
            self.stats['errors'] += 1
            return False
    
    def process_batch(self):
        """Process accumulated batch of messages"""
        if not self.message_batch:
            return
        
        try:
            batch_size = len(self.message_batch)
            logger.debug(f"Processing batch of {batch_size} messages")
            
            # Upsert to PostgreSQL silver table
            silver_count = self.pg_service.upsert_silver(self.message_batch)
            self.stats['silver_upserted'] += silver_count
            
            # Index to Elasticsearch
            es_count = self.es_service.bulk_index(self.message_batch)
            self.stats['es_indexed'] += es_count
            
            logger.info(f"📦 Processed batch: {silver_count} → PostgreSQL, {es_count} → Elasticsearch")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            self.stats['errors'] += batch_size
        
        finally:
            # Clear batch regardless of success/failure
            self.message_batch.clear()
    
    def start_consuming(self):
        """Start the main consumer loop"""
        if not self.setup():
            logger.error("Consumer setup failed")
            return
        
        self.running = True
        last_stats_time = time.time()
        last_batch_time = time.time()
        
        logger.info("🚀 Starting clean consumer loop...")
        
        try:
            while self.running:
                processed_count = self.kafka_service.consume_messages(
                    consumer=self.consumer,
                    message_handler=self.process_message,
                    timeout=1.0,
                    max_messages=100
                )
                
                # Force process batch every 10 seconds even if not full
                if time.time() - last_batch_time > 10 and self.message_batch:
                    self.process_batch()
                    last_batch_time = time.time()
                
                # Print stats every 30 seconds
                if time.time() - last_stats_time > 30:
                    self.print_stats()
                    last_stats_time = time.time()
                
                # Small sleep
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            # Process remaining batch
            if self.message_batch:
                self.process_batch()
            self.stop()
    
    def print_stats(self):
        """Print consumer statistics"""
        runtime = datetime.now() - self.stats['start_time']
        batch_pending = len(self.message_batch)
        
        logger.info(f"📊 Clean Consumer Stats - Runtime: {runtime}, "
                   f"Consumed: {self.stats['messages_consumed']}, "
                   f"PostgreSQL: {self.stats['silver_upserted']}, "
                   f"Elasticsearch: {self.stats['es_indexed']}, "
                   f"Pending: {batch_pending}, "
                   f"Errors: {self.stats['errors']}")
    
    def stop(self):
        """Stop the consumer gracefully"""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        self.pg_service.close()
        self.es_service.close()
        self.kafka_service.close()
        
        self.print_stats()
        logger.info("🛑 Clean consumer stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    global consumer_instance
    if consumer_instance:
        consumer_instance.stop()

if __name__ == "__main__":
    # Setup signal handlers
    consumer_instance = None
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    consumer_instance = CleanConsumer()
    consumer_instance.start_consuming()