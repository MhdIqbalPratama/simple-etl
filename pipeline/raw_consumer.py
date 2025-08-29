import sys
import os
import signal
import logging
import time
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.kafka_services import KafkaService
from services.staging_pg import PostgresService
from processor.cleaner import Cleaner
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('raw_consumer')


class RawConsumer:
    """Consumer for topic_raw -> archives to pg_bronze"""

    def __init__(self):
        self.kafka_service = KafkaService()
        self.pg_service = PostgresService()
        self.cleaner = Cleaner()
        self.consumer = None
        self.running = False

        # Processing statistics
        self.stats = {
            'messages_consumed': 0,
            'bronze_inserted': 0,
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

            # Create consumer
            group_id = os.getenv('KAFKA_GROUP_RAW', 'raw_consumer_group')
            self.consumer = self.kafka_service.get_consumer(
                group_id=group_id,
                topics=[self.kafka_service.topic_raw]
            )

            logger.info(f"Raw consumer initialized - Group: {group_id}, Topic: {self.kafka_service.topic_raw}")
            return True

        except Exception as e:
            logger.error(f"Setup failed: {e}")
            return False

    def process_messages(self, messages: list) -> int:
        """
        Validate and insert batch of raw messages into bronze
        Returns number of records inserted
        """
        try:
            if not messages:
                return 0

            articles = []
            for msg in messages:
                try:
                    if not isinstance(msg, dict):
                        logger.warning("Skipping non-dict message")
                        continue

                    if not msg.get('link'):
                        logger.warning("Skipping message without link")
                        continue

                    # Generate ID if missing
                    if not msg.get('id'):
                        msg['id'] = self.cleaner.generate_id(msg['link'])

                    articles.append(msg)

                except Exception as e:
                    logger.error(f"Error preparing message for bronze: {e}")
                    self.stats['errors'] += 1
                    continue

            if articles:
                inserted = self.pg_service.insert_bronze_lv(articles)
                self.stats['bronze_inserted'] += inserted
                logger.info(f"Inserted {inserted} records into bronze table")
                return inserted

            return 0

        except Exception as e:
            logger.error(f"Error in process_messages: {e}")
            self.stats['errors'] += 1
            return 0

    def start_consuming(self):
        """Start the main consumer loop"""
        if not self.setup():
            logger.error("Consumer setup failed")
            return

        self.running = True
        last_stats_time = time.time()

        logger.info("🚀 Starting raw consumer loop...")

        try:
            while self.running:
                batch = []

                # Consume up to 50 messages per poll
                processed_count = self.kafka_service.consume_messages(
                    consumer=self.consumer,
                    message_handler=lambda m: batch.append(m),
                    timeout=1.0,
                    max_messages=50
                )

                if processed_count > 0:
                    self.stats['messages_consumed'] += processed_count
                    self.process_messages(batch)

                # Print stats every 30 seconds
                if time.time() - last_stats_time > 30:
                    self.print_stats()
                    last_stats_time = time.time()

                # Small sleep to prevent CPU spinning
                time.sleep(0.1)

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}")
        finally:
            self.stop()

    def print_stats(self):
        """Print consumer statistics"""
        runtime = datetime.now() - self.stats['start_time']
        logger.info(
            f"📊 Raw Consumer Stats - Runtime: {runtime}, "
            f"Consumed: {self.stats['messages_consumed']}, "
            f"Bronze: {self.stats['bronze_inserted']}, "
            f"Errors: {self.stats['errors']}"
        )

    def stop(self):
        """Stop the consumer gracefully"""
        self.running = False

        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

        self.pg_service.close()
        self.kafka_service.close()

        self.print_stats()
        logger.info("🛑 Raw consumer stopped")


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

    consumer_instance = RawConsumer()
    consumer_instance.start_consuming()
