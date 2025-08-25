import asyncio
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from services.pg_staging import PGStagingService
from processor.cleaner import Cleaner
import logging
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class StreamingConsumer:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BROKER')
        self.topic = os.getenv('KAFKA_TOPIC')
        self.pg_service = PGStagingService()
        self.cleaner = Cleaner()
        self.consumer = None
        self.running = False
        
        # Processing counters
        self.stats = {
            'messages_consumed': 0,
            'bronze_inserted': 0,
            'silver_processed': 0,
            'gold_processed': 0,
            'errors': 0,
            'start_time': datetime.now()
        }
    
    def setup_consumer(self):
        """Initialize Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=[self.bootstrap_servers],
                auto_offset_reset='latest',  # Start from latest to avoid old messages
                enable_auto_commit=True,
                group_id='streaming-etl-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: json.loads(k.decode('utf-8')) if k else None,
                consumer_timeout_ms=5000,  # 5 second timeout
                max_poll_records=100  # Process in batches
            )
            logger.info("Kafka consumer initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False
    
    def setup_database(self):
        """Initialize database connection and tables"""
        try:
            if not self.pg_service.connect():
                return False
            
            if not self.pg_service.setup_staging_tables():
                return False
            
            # Create gold view
            self.create_gold_view()
            logger.info("Database setup completed")
            return True
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            return False
    
    def create_gold_view(self):
        """Create gold view from silver table with enhanced search capabilities"""
        try:
            with self.pg_service.conn.cursor() as cursor:
                cursor.execute("DROP VIEW IF EXISTS vw_gold CASCADE")
                
                gold_view_query = """
                CREATE VIEW vw_gold AS
                SELECT 
                    id,
                    title,
                    link,
                    image,
                    date,
                    topic,
                    content,
                    content_length,
                    -- Enhanced search text combining title and content
                    CONCAT(title, ' ', COALESCE(content, '')) as search_text,
                    -- Additional derived fields for analytics
                    CASE 
                        WHEN content_length < 500 THEN 'Short'
                        WHEN content_length < 1500 THEN 'Medium'
                        ELSE 'Long'
                    END as content_category,
                    EXTRACT(HOUR FROM date) as publish_hour,
                    EXTRACT(DOW FROM date) as publish_day_of_week,
                    DATE(date) as publish_date,
                    created_at,
                    -- Content quality indicators
                    CASE WHEN title IS NOT NULL AND LENGTH(title) > 10 THEN TRUE ELSE FALSE END as has_good_title,
                    CASE WHEN content_length > 200 THEN TRUE ELSE FALSE END as has_substantial_content
                FROM pg_silver 
                WHERE processed = TRUE
                  AND title IS NOT NULL 
                  AND content IS NOT NULL
                  AND date IS NOT NULL
                """
                
                cursor.execute(gold_view_query)
                
                # Create indexes on the underlying silver table for better view performance
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_silver_processed_date 
                    ON pg_silver (processed, date DESC) 
                    WHERE processed = TRUE
                """)
                
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_silver_topic_date 
                    ON pg_silver (topic, date DESC) 
                    WHERE processed = TRUE
                """)
                
                self.pg_service.conn.commit()
                logger.info("Gold view created successfully")
                
        except Exception as e:
            logger.error(f"Failed to create gold view: {e}")
            self.pg_service.conn.rollback()
    
    def process_to_bronze(self, messages):
        """Insert messages into bronze table"""
        try:
            if not messages:
                return 0
            
            # Prepare articles for bronze insertion
            articles = []
            for msg in messages:
                if isinstance(msg, dict) and 'link' in msg:
                    # Generate ID if not present
                    if 'id' not in msg:
                        msg['id'] = self.cleaner.generate_id(msg['link'])
                    articles.append(msg)
            
            if articles:
                inserted = self.pg_service.insert_bronze(articles)
                self.stats['bronze_inserted'] += inserted
                logger.info(f"Inserted {inserted} records into bronze table")
                return inserted
            
            return 0
            
        except Exception as e:
            logger.error(f"Error processing to bronze: {e}")
            self.stats['errors'] += 1
            return 0
    
    def process_bronze_to_silver(self):
        """Process data from bronze to silver using Cleaner"""
        try:
            processed = self.pg_service.process_bronze_to_silver()
            self.stats['silver_processed'] += processed
            if processed > 0:
                logger.info(f"Processed {processed} records from bronze to silver")
            return processed
        except Exception as e:
            logger.error(f"Error processing bronze to silver: {e}")
            self.stats['errors'] += 1
            return 0
    
    def update_gold_view_stats(self):
        """Update gold statistics (the view is automatically updated)"""
        try:
            with self.pg_service.conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM vw_gold")
                gold_count = cursor.fetchone()[0]
                self.stats['gold_processed'] = gold_count
                return gold_count
        except Exception as e:
            logger.error(f"Error getting gold stats: {e}")
            return 0
    
    def print_stats(self):
        """Print processing statistics"""
        runtime = datetime.now() - self.stats['start_time']
        
        print("\n" + "="*60)
        print(f"📊 STREAMING ETL PIPELINE STATS")
        print("="*60)
        print(f"🕐 Runtime: {runtime}")
        print(f"📨 Messages Consumed: {self.stats['messages_consumed']}")
        print(f"🥉 Bronze Inserted: {self.stats['bronze_inserted']}")
        print(f"🥈 Silver Processed: {self.stats['silver_processed']}")
        print(f"🥇 Gold Available: {self.stats['gold_processed']}")
        print(f"❌ Errors: {self.stats['errors']}")
        
        if self.stats['messages_consumed'] > 0:
            success_rate = ((self.stats['bronze_inserted'] + self.stats['silver_processed']) / 
                          (self.stats['messages_consumed'] * 2)) * 100
            print(f"✅ Success Rate: {success_rate:.1f}%")
        
        print("="*60)
    
    async def start_streaming(self):
        """Start the streaming consumer pipeline"""
        logger.info("🚀 Starting streaming ETL pipeline...")
        
        # Setup consumer and database
        if not self.setup_consumer():
            logger.error("Failed to setup consumer")
            return
        
        if not self.setup_database():
            logger.error("Failed to setup database")
            return
        
        self.running = True
        last_stats_print = time.time()
        stats_interval = 30  # Print stats every 30 seconds
        
        try:
            while self.running:
                try:
                    # Consume messages with timeout
                    message_batch = self.consumer.poll(timeout_ms=5000, max_records=50)
                    
                    if message_batch:
                        all_messages = []
                        
                        # Extract messages from all partitions
                        for partition_messages in message_batch.values():
                            for message in partition_messages:
                                all_messages.append(message.value)
                                self.stats['messages_consumed'] += 1
                        
                        # Process to bronze
                        if all_messages:
                            self.process_to_bronze(all_messages)
                            logger.info(f"Consumed {len(all_messages)} messages from Kafka")
                    
                    # Process bronze to silver (clean unprocessed bronze records)
                    self.process_bronze_to_silver()
                    
                    # Update gold view stats
                    self.update_gold_view_stats()
                    
                    # Print stats periodically
                    if time.time() - last_stats_print > stats_interval:
                        self.print_stats()
                        last_stats_print = time.time()
                    
                    # Small sleep to prevent excessive CPU usage
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"Error in streaming loop: {e}")
                    self.stats['errors'] += 1
                    await asyncio.sleep(5)  # Wait before retrying
        
        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")
        
        finally:
            self.stop()
    
    def stop(self):
        """Stop the streaming consumer"""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")
        
        if self.pg_service:
            self.pg_service.close()
            logger.info("Database connection closed")
        
        self.print_stats()
        logger.info("🛑 Streaming pipeline stopped")

# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Streaming ETL Pipeline Consumer')
    parser.add_argument('--stats-interval', type=int, default=30, 
                       help='Statistics print interval in seconds')
    
    args = parser.parse_args()
    
    async def main():
        consumer = StreamingConsumer()
        
        try:
            await consumer.start_streaming()
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
        finally:
            consumer.stop()
    
    # Run the streaming pipeline
    asyncio.run(main())