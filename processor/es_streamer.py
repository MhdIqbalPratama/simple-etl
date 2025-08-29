import sys
import os
import signal
import logging
import time
from datetime import datetime
from typing import List, Dict, Any

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.kafka_services import KafkaService
from services.es import ElasticsearchService
from processor.cleaner import Cleaner
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('es_streamer')

class ElasticsearchStreamer:
    """Streaming service: clean-topic → Elasticsearch"""
    
    def __init__(self):
        self.kafka_service = KafkaService()
        self.es_service = ElasticsearchService()
        self.cleaner = Cleaner()
        self.consumer = None
        self.running = False
        
        # Batch processing for efficiency
        self.batch_size = int(os.getenv('ES_BATCH_SIZE', '10'))
        self.message_batch = []
        self.last_flush_time = time.time()
        self.flush_interval = int(os.getenv('ES_FLUSH_INTERVAL', '5'))  # seconds
        
        # Processing statistics
        self.stats = {
            'messages_consumed': 0,
            'messages_indexed': 0,
            'batch_processed': 0,
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
            
            # Setup Elasticsearch
            if not self.es_service.setup_index():
                logger.error("Failed to setup Elasticsearch index")
                return False
            
            # Create consumer for clean topic
            group_id = 'es_streamer_group'
            self.consumer = self.kafka_service.get_consumer(
                group_id=group_id,
                topics=[self.kafka_service.topic_raw]
            )
            
            logger.info(f"ES Streamer initialized - Group: {group_id}, Batch: {self.batch_size}")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False
    
    def process_message(self, message: dict) -> bool:
        """Add message to batch for Elasticsearch indexing"""
        try:
            # Validate message
            if not message.get('link') or not message.get('content'):
                logger.warning("Skipping message missing required fields")
                self.stats['errors'] += 1
                return True  # Continue processing
            
            # Add to batch
            message = self.cleaner.clean_article(message)
            self.message_batch.append(message)
            self.stats['messages_consumed'] += 1
            
            # Process batch when it reaches size limit or time interval
            current_time = time.time()
            if (len(self.message_batch) >= self.batch_size or 
                current_time - self.last_flush_time >= self.flush_interval):
                self.process_batch()
                self.last_flush_time = current_time
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding message to batch: {e}")
            self.stats['errors'] += 1
            return False
    
    def process_batch(self):
        """Process accumulated batch of messages to Elasticsearch"""
        if not self.message_batch:
            return
        
        try:
            batch_size = len(self.message_batch)
            logger.debug(f"Processing batch of {batch_size} messages")
            
            # Index batch to Elasticsearch
            indexed_count = self.es_service.bulk_index(self.message_batch)
            
            self.stats['messages_indexed'] += indexed_count
            self.stats['batch_processed'] += 1
            
            success_rate = round((indexed_count / batch_size) * 100, 2) if batch_size > 0 else 0
            logger.info(f"Indexed batch: {indexed_count}/{batch_size} docs ({success_rate}%)")
            
            if indexed_count < batch_size:
                failed_count = batch_size - indexed_count
                self.stats['errors'] += failed_count
                logger.warning(f"Failed to index {failed_count} documents")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            self.stats['errors'] += len(self.message_batch)
        
        finally:
            # Clear batch regardless of success/failure
            self.message_batch.clear()
    
    def start_streaming(self):
        """Start the main streaming loop"""
        if not self.setup():
            logger.error("Streamer setup failed")
            return
        
        self.running = True
        last_stats_time = time.time()
        
        logger.info("Starting ES streaming service...")
        
        try:
            while self.running:
                processed_count = self.kafka_service.consume_messages(
                    consumer=self.consumer,
                    message_handler=self.process_message,
                    timeout=1.0,
                    max_messages=100
                )
                
                # Force process batch if time interval exceeded
                current_time = time.time()
                if (self.message_batch and 
                    current_time - self.last_flush_time >= self.flush_interval):
                    self.process_batch()
                    self.last_flush_time = current_time
                
                # Print stats every 30 seconds
                if current_time - last_stats_time > 30:
                    self.print_stats()
                    last_stats_time = current_time
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Streamer interrupted by user")
        except Exception as e:
            logger.error(f"Streamer error: {e}")
        finally:
            # Process remaining batch before shutdown
            if self.message_batch:
                logger.info(f"Processing final batch of {len(self.message_batch)} messages")
                self.process_batch()
            self.stop()
    
    def print_stats(self):
        """Print streamer statistics"""
        runtime = datetime.now() - self.stats['start_time']
        batch_pending = len(self.message_batch)
        
        # Calculate rates
        success_rate = 0
        if self.stats['messages_consumed'] > 0:
            success_rate = round((self.stats['messages_indexed'] / self.stats['messages_consumed']) * 100, 2)
        
        avg_batch_size = 0
        if self.stats['batch_processed'] > 0:
            avg_batch_size = round(self.stats['messages_indexed'] / self.stats['batch_processed'], 1)
        
        logger.info(
            f"ES Streamer Stats - Runtime: {runtime}, "
            f"Consumed: {self.stats['messages_consumed']}, "
            f"Indexed: {self.stats['messages_indexed']}, "
            f"Batches: {self.stats['batch_processed']}, "
            f"Pending: {batch_pending}, "
            f"Success Rate: {success_rate}%, "
            f"Avg Batch: {avg_batch_size}, "
            f"Errors: {self.stats['errors']}"
        )
    
    def health_check(self) -> Dict[str, Any]:
        """Check service health"""
        try:
            # Check Elasticsearch health
            es_health = self.es_service.health_check()
            
            # Check Kafka health
            kafka_health = self.kafka_service.health_check()
            
            # Get basic stats
            es_stats = self.es_service.get_statistics()
            
            return {
                "status": "healthy" if es_health.get('cluster_status') == 'green' else "degraded",
                "elasticsearch": es_health,
                "kafka": kafka_health,
                "processing_stats": self.stats,
                "batch_pending": len(self.message_batch),
                "es_document_count": es_stats.get('total_documents', 0)
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def stop(self):
        """Stop the streamer gracefully"""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        self.es_service.close()
        self.kafka_service.close()
        
        self.print_stats()
        logger.info("ES Streamer stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    global streamer_instance
    if streamer_instance:
        streamer_instance.stop()

if __name__ == "__main__":
    # Setup signal handlers for graceful shutdown
    streamer_instance = None
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    streamer_instance = ElasticsearchStreamer()
    streamer_instance.start_streaming()