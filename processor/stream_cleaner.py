import sys
import os
import signal
import logging
import time
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.kafka_services import KafkaService
from processor.cleaner import Cleaner
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('streaming_cleaner')

class StreamingCleanerProcessor:
    """Streaming processor: raw-topic → clean-topic"""
    
    def __init__(self):
        self.kafka_service = KafkaService()
        self.cleaner = Cleaner()
        self.consumer = None
        self.running = False
        
        # Processing statistics
        self.stats = {
            'messages_consumed': 0,
            'messages_cleaned': 0,
            'messages_produced': 0,
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
            
            # Create consumer for topic_raw
            group_id = 'cleaner_processor_group'
            self.consumer = self.kafka_service.get_consumer(
                group_id=group_id,
                topics=[self.kafka_service.topic_raw]
            )
            
            logger.info(f"Streaming cleaner initialized - Group: {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False
    
    def process_message(self, message: dict) -> bool:
        """Process raw message and produce to clean topic"""
        try:
            # Validation
            if not message.get('link'):
                logger.warning("Dropping message without link")
                self.stats['errors'] += 1
                return True  # Return True to continue processing
            
            # Generate ID if missing
            if not message.get('id'):
                message['id'] = self.cleaner.generate_id(message['link'])
            
            # Clean the article
            cleaned_message = self.cleaner.clean_article(message)
            
            # Add processing metadata
            cleaned_message['processed_at'] = datetime.now().isoformat()
            cleaned_message['pipeline_stage'] = 'clean'
            
            # Produce to clean topic
            success = self.kafka_service.produce_to_clean(
                cleaned_message, 
                key=cleaned_message['id']
            )
            
            if success:
                self.stats['messages_cleaned'] += 1
                self.stats['messages_produced'] += 1
                logger.debug(f"Cleaned and sent to clean-topic: {cleaned_message['id']}")
                return True
            else:
                logger.error(f"Failed to produce to clean topic: {cleaned_message['id']}")
                self.stats['errors'] += 1
                return False
                
        except Exception as e:
            logger.error(f"Error cleaning message: {e}")
            self.stats['errors'] += 1
            return False
    
    def start_processing(self):
        """Start the main streaming processor loop"""
        if not self.setup():
            logger.error("Processor setup failed")
            return
        
        self.running = True
        last_stats_time = time.time()
        
        logger.info("Starting streaming cleaner processor...")
        
        try:
            while self.running:
                processed_count = self.kafka_service.consume_messages(
                    consumer=self.consumer,
                    message_handler=self.process_message,
                    timeout=1.0,
                    max_messages=50
                )
                
                if processed_count > 0:
                    self.stats['messages_consumed'] += processed_count
                
                # Print stats every 30 seconds
                if time.time() - last_stats_time > 30:
                    self.print_stats()
                    last_stats_time = time.time()
                
                # Small sleep to prevent CPU spinning
                time.sleep(0.1)
                
        except KeyboardInterrupt:
            logger.info("Processor interrupted by user")
        except Exception as e:
            logger.error(f"Processor error: {e}")
        finally:
            self.stop()
    
    def print_stats(self):
        """Print processor statistics"""
        runtime = datetime.now() - self.stats['start_time']
        success_rate = 0
        if self.stats['messages_consumed'] > 0:
            success_rate = round((self.stats['messages_cleaned'] / self.stats['messages_consumed']) * 100, 2)
        
        logger.info(
            f"Streaming Cleaner Stats - Runtime: {runtime}, "
            f"Consumed: {self.stats['messages_consumed']}, "
            f"Cleaned: {self.stats['messages_cleaned']}, "
            f"Produced: {self.stats['messages_produced']}, "
            f"Success Rate: {success_rate}%, "
            f"Errors: {self.stats['errors']}"
        )
    
    def stop(self):
        """Stop the processor gracefully"""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        # Flush producer before closing
        if self.kafka_service.producer:
            self.kafka_service.producer.flush(timeout=10)
        
        self.kafka_service.close()
        
        self.print_stats()
        logger.info("Streaming cleaner processor stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    global processor_instance
    if processor_instance:
        processor_instance.stop()

if __name__ == "__main__":
    # Setup signal handlers for graceful shutdown
    processor_instance = None
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    processor_instance = StreamingCleanerProcessor()
    processor_instance.start_processing()