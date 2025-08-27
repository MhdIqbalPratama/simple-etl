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
            'gold_available': 0,
            'errors': 0,
            'start_time': datetime.now(),
            'last_processing_time': None
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
                max_poll_records=100,  # Process in batches
                fetch_min_bytes=1,
                fetch_max_wait_ms=500
            )
            logger.info(f"Kafka consumer initialized successfully for topic: {self.topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize Kafka consumer: {e}")
            return False
    
    def setup_database(self):
        """Initialize database connection and tables"""
        try:
            if not self.pg_service.connect():
                logger.error("Failed to connect to database")
                return False
            
            if not self.pg_service.setup_staging_tables():
                logger.error("Failed to setup staging tables")
                return False
            
            logger.info("Database setup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            return False
    
    def process_to_bronze(self, messages):
        """Insert messages into bronze table"""
        try:
            if not messages:
                return 0
            
            # Prepare articles for bronze insertion
            articles = []
            for msg in messages:
                try:
                    if isinstance(msg, dict):
                        # Ensure we have essential fields
                        if not msg.get('link'):
                            logger.warning("Skipping message without link")
                            continue
                            
                        # Generate ID if not present
                        if 'id' not in msg or not msg['id']:
                            msg['id'] = self.cleaner.generate_id(msg['link'])
                        
                        articles.append(msg)
                except Exception as e:
                    logger.error(f"Error processing message for bronze: {e}")
                    self.stats['errors'] += 1
                    continue
            
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
    
    def process_silver_to_gold(self):
        """Mark silver records as processed (making them available in gold view)"""
        try:
            processed = self.pg_service.process_silver_to_gold()
            if processed > 0:
                logger.info(f"Marked {processed} silver records as processed (available in gold view)")
            return processed
        except Exception as e:
            logger.error(f"Error processing silver to gold: {e}")
            self.stats['errors'] += 1
            return 0
    
    def update_gold_stats(self):
        """Update gold statistics from the view"""
        try:
            with self.pg_service.conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM vw_gold")
                gold_count = cursor.fetchone()[0]
                self.stats['gold_available'] = gold_count
                return gold_count
        except Exception as e:
            logger.error(f"Error getting gold stats: {e}")
            return 0
    
    def get_pipeline_health(self):
        """Get current pipeline health metrics"""
        try:
            health = self.pg_service.get_pipeline_health()
            return health
        except Exception as e:
            logger.error(f"Error getting pipeline health: {e}")
            return {}
    
    def print_stats(self):
        """Print processing statistics"""
        runtime = datetime.now() - self.stats['start_time']
        
        print("\n" + "="*70)
        print(f"📊 STREAMING ETL PIPELINE STATS")
        print("="*70)
        print(f"🕐 Runtime: {runtime}")
        print(f"📨 Messages Consumed: {self.stats['messages_consumed']}")
        print(f"🥉 Bronze Inserted: {self.stats['bronze_inserted']}")
        print(f"🥈 Silver Processed: {self.stats['silver_processed']}")
        print(f"🥇 Gold Available: {self.stats['gold_available']}")
        print(f"❌ Errors: {self.stats['errors']}")
        
        if self.stats['messages_consumed'] > 0:
            success_rate = (self.stats['bronze_inserted'] / self.stats['messages_consumed']) * 100
            print(f"✅ Success Rate: {success_rate:.1f}%")
        
        if self.stats['last_processing_time']:
            print(f"⏰ Last Processing: {self.stats['last_processing_time'].strftime('%H:%M:%S')}")
        
        # Pipeline health
        health = self.get_pipeline_health()
        if health:
            print(f"🏥 Pipeline Status: {health.get('pipeline_status', 'unknown').upper()}")
            print(f"⚡ Processing Efficiency: {health.get('processing_efficiency', 0):.1f}%")
            print(f"📊 Quality Score: {health.get('quality_score', 0):.1f}%")
        
        print("="*70)
    
    def print_detailed_stats(self):
        """Print detailed pipeline statistics"""
        health = self.get_pipeline_health()
        
        if health:
            print("\n" + "="*70)
            print("📈 DETAILED PIPELINE ANALYTICS")
            print("="*70)
            
            # Bronze layer stats
            bronze = health.get('bronze', {})
            print(f"🥉 BRONZE LAYER:")
            print(f"   Total: {bronze.get('total', 0)}")
            print(f"   Processed: {bronze.get('processed', 0)}")
            print(f"   Pending: {bronze.get('pending', 0)}")
            
            # Silver layer stats  
            silver = health.get('silver', {})
            print(f"🥈 SILVER LAYER:")
            print(f"   Total: {silver.get('total', 0)}")
            print(f"   Processed: {silver.get('processed', 0)}")
            print(f"   Pending: {silver.get('pending', 0)}")
            print(f"   Avg Content Length: {silver.get('avg_content_length', 0):.0f}")
            
            # Gold layer stats
            gold = health.get('gold', {})
            print(f"🥇 GOLD LAYER (VIEW):")
            print(f"   Total Articles: {gold.get('total_articles', 0)}")
            print(f"   Recent Articles: {gold.get('recent_articles', 0)}")
            print(f"   Good Titles: {gold.get('good_titles', 0)}")
            print(f"   Substantial Content: {gold.get('substantial_content', 0)}")
            print(f"   Avg Content Length: {gold.get('avg_content_length', 0):.0f}")
            
            # Topic breakdown
            topics = gold.get('by_topic', [])
            if topics:
                print(f"📊 TOP TOPICS:")
                for topic in topics[:5]:  # Show top 5
                    print(f"   {topic['topic']}: {topic['count']} articles")
            
            print("="*70)
    
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
        last_detailed_stats = time.time()
        stats_interval = 30  # Print stats every 30 seconds
        detailed_stats_interval = 300  # Print detailed stats every 5 minutes
        
        logger.info("✅ Pipeline setup completed, starting message processing...")
        
        try:
            message_count = 0
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
                                message_count += 1
                        
                        # Process to bronze
                        if all_messages:
                            bronze_inserted = self.process_to_bronze(all_messages)
                            logger.info(f"Consumed {len(all_messages)} messages from Kafka, inserted {bronze_inserted} to bronze")
                    
                    # Process bronze to silver (clean unprocessed bronze records)
                    silver_processed = self.process_bronze_to_silver()
                    
                    # Process silver to gold (mark silver records as processed)
                    gold_processed = self.process_silver_to_gold()
                    
                    # Update stats
                    self.update_gold_stats()
                    self.stats['last_processing_time'] = datetime.now()
                    
                    # Print stats periodically
                    current_time = time.time()
                    if current_time - last_stats_print > stats_interval:
                        self.print_stats()
                        last_stats_print = current_time
                    
                    # Print detailed stats periodically
                    if current_time - last_detailed_stats > detailed_stats_interval:
                        self.print_detailed_stats()
                        last_detailed_stats = current_time
                    
                    # Small sleep to prevent excessive CPU usage when no messages
                    if not message_batch:
                        await asyncio.sleep(1)
                    else:
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
        logger.info("🛑 Stopping streaming pipeline...")
        self.running = False
        
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Kafka consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        
        if self.pg_service:
            try:
                self.pg_service.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database: {e}")
        
        # Final stats
        self.print_stats()
        self.print_detailed_stats()
        logger.info("🏁 Streaming pipeline stopped successfully")

# CLI interface
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Streaming ETL Pipeline Consumer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python consumer_pipeline.py                    # Run with default settings
  python consumer_pipeline.py --stats-interval 60  # Print stats every 60 seconds
  python consumer_pipeline.py --help             # Show this help message

Environment Variables Required:
  KAFKA_BROKER    - Kafka broker address (e.g., localhost:9092)
  KAFKA_TOPIC     - Kafka topic to consume from
  PG_HOST         - PostgreSQL host
  PG_DATABASE     - PostgreSQL database name
  PG_USER         - PostgreSQL username
  PG_PASSWORD     - PostgreSQL password
  PG_PORT         - PostgreSQL port
        """
    )
    
    parser.add_argument(
        '--stats-interval', 
        type=int, 
        default=30,
        help='Statistics print interval in seconds (default: 30)'
    )
    
    parser.add_argument(
        '--detailed-stats-interval',
        type=int,
        default=300,
        help='Detailed statistics print interval in seconds (default: 300)'
    )
    
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
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    except Exception as e:
        logger.error(f"Failed to start pipeline: {e}")