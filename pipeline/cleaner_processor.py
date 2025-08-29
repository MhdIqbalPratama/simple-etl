import sys
import os
import signal
import logging
import time
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.kafka_services import KafkaService
from processor.cleaner import Cleaner
from services.staging_pg import PostgresService
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('cleaner_processor')

class CleanerProcessor:
    
    def __init__(self):
        self.kafka_service = KafkaService()
        self.cleaner = Cleaner()
        self.staging = PostgresService()
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
            
            logger.info(f"Cleaner processor initialized - Group: {group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False
    
    def process_message(self, message: dict):
        """Process raw message into clean message for silver"""
        try:
            # 1. Validation
            if not message.get('link'):
                logger.warning("Dropping message without link")
                return True  # Skip aja, anggap sukses biar gak spam warning Kafka

            # 2. Generate ID kalau belum ada
            if 'id' not in message or not message['id']:
                message['id'] = self.cleaner.generate_id(message['link'])
            
            message = self.cleaner.clean_article(message)

            # 4. Insert ke silver
            self.staging.upsert_silver([message])

            # 5. Produce ke clean-topic
            self.kafka_service.produce_to_clean(message)

            logger.info(f"Processed and sent to silver: {message['id']}")
            return True
        except Exception as e:
            logger.error(f"Error cleaning message: {e}")
            return False
    
    def start_processing(self):
        """Start the main processor loop"""
        if not self.setup():
            logger.error("Processor setup failed")
            return
        
        self.running = True
        last_stats_time = time.time()
        
        logger.info("🚀 Starting cleaner processor loop...")
        
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
                
                # Small sleep
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
        logger.info(f"Cleaner Processor Stats - Runtime: {runtime}, "
                   f"Consumed: {self.stats['messages_consumed']}, "
                   f"Cleaned: {self.stats['messages_cleaned']}, "
                   f"Produced: {self.stats['messages_produced']}, "
                   f"Errors: {self.stats['errors']}")
    
    def stop(self):
        """Stop the processor gracefully"""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        self.kafka_service.close()
        
        self.print_stats()
        logger.info("Cleaner processor stopped")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, shutting down...")
    global processor_instance
    if processor_instance:
        processor_instance.stop()

if __name__ == "__main__":
    # Setup signal handlers
    processor_instance = None
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    processor_instance = CleanerProcessor()
    processor_instance.start_processing()