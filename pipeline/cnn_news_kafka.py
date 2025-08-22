import asyncio
import time
from crawler.cnn import CNNCrawler
from services.es_services import ESService
from services.pg_service import PGService
from services.producer_services import ProducerService
from services.consumer_services import ConsumerService
from services.pg_staging import PGStagingService
from processor.cleaner import Cleaner
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class ETLProcessKafka:
    def __init__(self, es_cloud=os.getenv("ES_CLOUD_URL"), es_api_key=os.getenv("ES_API_KEY"),
                 pg_host=os.getenv("PG_HOST"), pg_database=os.getenv("PG_DATABASE"),
                 pg_user=os.getenv("PG_USER"), pg_password=os.getenv("PG_PASSWORD"),
                 pg_port=os.getenv("PG_PORT")):
        
        # Initialize components
        self.crawler = CNNCrawler()
        self.cleaner = Cleaner()
        self.kafka_producer = ProducerService()
        self.kafka_consumer = ConsumerService()
        
        # Initialize services
        self.es_service = ESService()
        self.pg_service = PGService(pg_host, pg_database, pg_user, pg_password, pg_port)
        self.pg_staging = PGStagingService(pg_host, pg_database, pg_user, pg_password, pg_port)
    
    def setup_services(self):
        """Setup all required services and tables"""
        print("Setting up Elasticsearch...")
        self.es_service.setup_index()
        
        print("Setting up PostgreSQL main tables...")
        self.pg_service.setup_table()
        
        print("Setting up PostgreSQL staging tables...")
        self.pg_staging.setup_staging_tables()
        
        print("All services setup completed!")
    
    async def run_complete_etl(self, total_page=5):
        """Run the complete ETL pipeline with Kafka"""
        try:
            # Phase 1: Extract and Send to Kafka
            print("\nPhase 1: Extract and send to Kafka")
            print("=====================================")
            
            raw_articles = await self.crawler.crawl_all_contents(total_page=total_page)
            print(f"Crawled {len(raw_articles)} articles")
            
            if not raw_articles:
                print("No articles found. Stopping pipeline.")
                return None
            
            # Pre-process articles to ensure they have IDs
            import hashlib
            for article in raw_articles:
                if not article.get('id') and article.get('link'):
                    article['id'] = hashlib.md5(article['link'].encode()).hexdigest()
                elif not article.get('id'):
                    # Generate ID from title + timestamp if no link
                    import time
                    unique_str = f"{article.get('title', 'unknown')}_{time.time()}"
                    article['id'] = hashlib.md5(unique_str.encode()).hexdigest()
            
            # Send to Kafka
            kafka_sent = self.kafka_producer.send_batch(raw_articles)
            print(f"Sent {kafka_sent} messages to Kafka")
            
            # Small delay to ensure messages are available
            time.sleep(2)
            
            # Phase 2: Kafka to Bronze
            print("\nPhase 2: Kafka to Bronze")
            print("============================")
            
            # Consume from Kafka
            kafka_messages = self.kafka_consumer.consume_batch(timeout_ms=10000, max_messages=1000)
            print(f"Consumed {len(kafka_messages)} messages from Kafka")
            
            # Ensure consumed messages have IDs
            for message in kafka_messages:
                if not message.get('id') and message.get('link'):
                    message['id'] = hashlib.md5(message['link'].encode()).hexdigest()
                elif not message.get('id'):
                    import time
                    unique_str = f"{message.get('title', 'unknown')}_{time.time()}"
                    message['id'] = hashlib.md5(unique_str.encode()).hexdigest()
            
            # Save to Bronze
            bronze_saved = self.pg_staging.insert_bronze(kafka_messages)
            print(f"Saved {bronze_saved} records to Bronze table")
            
            # Phase 3: Bronze to Silver
            print("\nPhase 3: Bronze to Silver")
            print("=============================")
            
            silver_processed = self.pg_staging.process_bronze_to_silver()
            print(f"Processed {silver_processed} records to Silver table")
            
            # Phase 4: Silver to Gold
            print("\nPhase 4: Silver to Gold")
            print("===========================")
            
            gold_processed = self.pg_staging.process_silver_to_gold()
            print(f"Processed {gold_processed} records to Gold table")
            
            # Phase 5: Gold to Final Storage
            print("\nPhase 5: Gold to Final Storage")
            print("==================================")
            
            final_storage_result = await self.save_gold_to_final_storage()
            
            print(f"Pipeline Results Summary:")
            print(f"  - Raw articles: {len(raw_articles)}")
            print(f"  - Kafka sent: {kafka_sent}")
            print(f"  - Bronze saved: {bronze_saved}")
            print(f"  - Silver processed: {silver_processed}")
            print(f"  - Gold processed: {gold_processed}")
            print(f"  - Elasticsearch saved: {final_storage_result.get('es_saved', 0)}")
            print(f"  - PostgreSQL saved: {final_storage_result.get('pg_saved', 0)}")
            
            return {
                "crawled": len(raw_articles),
                "kafka_sent": kafka_sent,
                "bronze_saved": bronze_saved,
                "silver_processed": silver_processed,
                "gold_processed": gold_processed,
                "es_saved": final_storage_result.get('es_saved', 0),
                "pg_saved": final_storage_result.get('pg_saved', 0)
            }
            
        except Exception as e:
            print(f"Error in ETL pipeline: {e}")
            return None
    
    async def save_gold_to_final_storage(self):
        """Save data from Gold table to Elasticsearch and final PostgreSQL"""
        try:
            # Get today's gold records
            self.pg_staging.connect()
            with self.pg_staging.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, title, link, image, date, topic, content, content_length, search_text
                    FROM pg_gold 
                    WHERE DATE(created_at) = CURRENT_DATE
                    ORDER BY created_at DESC
                """)
                gold_records = cursor.fetchall()
            
            if not gold_records:
                print("No gold records found for today")
                return {"es_saved": 0, "pg_saved": 0}
            
            # Convert to article format
            articles = []
            for record in gold_records:
                article = {
                    'id': record[0],
                    'title': record[1],
                    'link': record[2],
                    'image': record[3],
                    'date': record[4],
                    'topic': record[5],
                    'content': record[6],
                    'content_length': record[7],
                    'search_text': record[8]
                }
                articles.append(article)
            
            print(f"Processing {len(articles)} articles from Gold table")
            
            # Save to Elasticsearch
            print("Saving to Elasticsearch...")
            es_success = self.es_service.save_bulk(articles)
            
            # Save to final PostgreSQL table
            print("Saving to PostgreSQL...")
            pg_success = self.pg_service.save_bulk(articles)
            
            return {"es_saved": es_success, "pg_saved": pg_success}
            
        except Exception as e:
            print(f"Error saving to final storage: {e}")
            return {"es_saved": 0, "pg_saved": 0}
    
    def get_statistics(self):
        """Get comprehensive statistics from the system"""
        try:
            # Get stats from Gold table (most complete)
            gold_stats = self.pg_staging.get_gold_stats()
            
            # Get stats from final PostgreSQL table
            pg_stats = self.pg_service.get_stats()
            
            # Combine stats
            combined_stats = {
                "gold_table": gold_stats,
                "final_table": pg_stats,
                "total_articles": max(
                    gold_stats.get('total_articles', 0) if gold_stats else 0,
                    pg_stats.get('total_articles', 0) if pg_stats else 0
                ),
                "recent_articles": max(
                    gold_stats.get('recent_articles', 0) if gold_stats else 0,
                    pg_stats.get('recent_articles', 0) if pg_stats else 0
                ),
                "by_topic": gold_stats.get('by_topic', []) if gold_stats else []
            }
            
            return combined_stats
            
        except Exception as e:
            print(f"Error getting statistics: {e}")
            return {}
    
    def cleanup(self):
        """Clean up all connections and resources"""
        try:
            self.kafka_producer.close()
            self.kafka_consumer.close()
            self.pg_staging.close()
            self.pg_service.close()
            print("🧹 Cleanup completed")
        except Exception as e:
            print(f"Error during cleanup: {e}")

    def test_kafka_connection(self):
        """Test Kafka connection and topic"""
        try:
            # Test producer
            test_message = {"test": "connection", "timestamp": time.time()}
            success = self.kafka_producer.send_message(test_message, key="test")
            
            if success:
                print("Kafka producer connection successful")
                
                # Test consumer
                time.sleep(1)  # Wait for message
                messages = self.kafka_consumer.consume_batch(timeout_ms=5000, max_messages=1)
                
                if messages:
                    print("Kafka consumer connection successful")
                    return True
                else:
                    print("Kafka consumer didn't receive test message")
                    return False
            else:
                print("Kafka producer connection failed")
                return False
                
        except Exception as e:
            print(f"Kafka connection test failed: {e}")
            return False
    
    def health_check(self):
        """Perform a health check on all components"""
        print("Performing health check...")
        
        health_status = {
            "elasticsearch": False,
            "postgresql": False,
            "kafka": False,
            "staging_tables": False
        }
        
        try:
            # Check Elasticsearch
            if self.es_service.es.ping():
                health_status["elasticsearch"] = True
                print("Elasticsearch: Connected")
            else:
                print("Elasticsearch: Connection failed")
            
            # Check PostgreSQL
            if self.pg_service.connect():
                health_status["postgresql"] = True
                print("PostgreSQL: Connected")
            else:
                print("PostgreSQL: Connection failed")
            
            # Check staging tables
            if self.pg_staging.connect():
                health_status["staging_tables"] = True
                print("PostgreSQL Staging: Connected")
            else:
                print("PostgreSQL Staging: Connection failed")
            
            # Check Kafka
            health_status["kafka"] = self.test_kafka_connection()
            
        except Exception as e:
            print(f"Health check error: {e}")
        
        return health_status