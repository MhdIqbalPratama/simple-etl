import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
import asyncio
from dotenv import load_dotenv

# Import your custom services
from crawler.cnn import CNNCrawler
from services.es_services import ESService
from services.pg_service import PGService
from services.producer_services import ProducerService
from services.consumer_services import ConsumerService
from services.pg_staging import PGStagingService
from processor.cleaner import Cleaner

default_args = {
    "owner": "iqbale",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "extract_cnn_with_kafka",
    default_args=default_args,
    description="Extract CNN news data with Kafka and staging",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1
)

# --- FUNCTIONS ---

def setup_services(**kwargs):
    """Setup database services and Kafka topic"""
    load_dotenv()
    
    # Setup Elasticsearch
    es_service = ESService()
    es_service.setup_index()
    
    # Setup PostgreSQL main tables
    pg_service = PGService(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT"),
    )
    pg_service.setup_table()
    
    # Setup PostgreSQL staging tables
    pg_staging = PGStagingService()
    pg_staging.setup_staging_tables()
    pg_staging.close()
    
    print("All services setup completed.")


def crawl_and_send_to_kafka(**kwargs):
    """Crawl CNN news and send to Kafka"""
    crawler = CNNCrawler()
    kafka_producer = ProducerService()
    
    try:
        # Crawl articles
        total_page = 5  # configurable
        articles = asyncio.run(crawler.crawl_all_contents(total_page))

        if not articles:
            print("No articles found.")
            return 0

        print(f"Total articles crawled: {len(articles)}")

        # Send to Kafka
        success_count = kafka_producer.send_batch(articles)
        print(f"Sent {success_count} articles to Kafka")
        
        return success_count
        
    except Exception as e:
        print(f"Error in crawl and send: {e}")
        raise
    finally:
        kafka_producer.close()


def consume_from_kafka_to_bronze(**kwargs):
    """Consume messages from Kafka and save to bronze table"""
    kafka_consumer = ConsumerService()
    pg_staging = PGStagingService()
    
    try:
        # Consume messages from Kafka
        messages = kafka_consumer.consume_batch(timeout_ms=30000, max_messages=1000)
        
        if not messages:
            print("No messages found in Kafka")
            return 0
        
        print(f"Consumed {len(messages)} messages from Kafka")
        
        # Generate IDs for messages that don't have them
        import hashlib
        for message in messages:
            if not message.get('id') and message.get('link'):
                message['id'] = hashlib.md5(message['link'].encode()).hexdigest()
            elif not message.get('id'):
                # Generate ID from title + timestamp if no link
                import time
                unique_str = f"{message.get('title', 'unknown')}_{time.time()}"
                message['id'] = hashlib.md5(unique_str.encode()).hexdigest()
        
        # Save to bronze table
        success_count = pg_staging.insert_bronze(messages)
        print(f"Saved {success_count} records to bronze table")
        
        return success_count
        
    except Exception as e:
        print(f"Error consuming from Kafka: {e}")
        raise
    finally:
        kafka_consumer.close()
        pg_staging.close()


def process_bronze_to_silver(**kwargs):
    """Process data from bronze to silver table"""
    pg_staging = PGStagingService()
    
    try:
        success_count = pg_staging.process_bronze_to_silver()
        print(f"Processed {success_count} records to silver table")
        return success_count
        
    except Exception as e:
        print(f"Error processing bronze to silver: {e}")
        raise
    finally:
        pg_staging.close()


def process_silver_to_gold(**kwargs):
    """Process data from silver to gold table"""
    pg_staging = PGStagingService()
    
    try:
        success_count = pg_staging.process_silver_to_gold()
        print(f"Processed {success_count} records to gold table")
        return success_count
        
    except Exception as e:
        print(f"Error processing silver to gold: {e}")
        raise
    finally:
        pg_staging.close()


def save_gold_to_final_storage(**kwargs):
    """Save gold data to Elasticsearch and final PostgreSQL table"""
    load_dotenv()
    
    pg_staging = PGStagingService()
    es_service = ESService()
    pg_service = PGService(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT"),
    )
    
    try:
        # Get data from gold table
        pg_staging.connect()
        with pg_staging.conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, title, link, image, date, topic, content, content_length, search_text
                FROM pg_gold 
                WHERE created_at >= CURRENT_DATE
            """)
            gold_records = cursor.fetchall()
        
        if not gold_records:
            print("No gold records found for today")
            return {"es_saved": 0, "pg_saved": 0}
        
        # Convert to dictionary format
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
        
        # Save to Elasticsearch
        es_success = es_service.save_bulk(articles)
        
        # Save to final PostgreSQL table
        pg_success = pg_service.save_bulk(articles)
        
        print(f"Saved {es_success} articles to Elasticsearch")
        print(f"Saved {pg_success} articles to PostgreSQL")
        
        return {"es_saved": es_success, "pg_saved": pg_success}
        
    except Exception as e:
        print(f"Error saving to final storage: {e}")
        raise
    finally:
        pg_staging.close()
        pg_service.close()


def generate_analytics(**kwargs):
    """Generate analytics from gold table"""
    pg_staging = PGStagingService()
    
    try:
        stats = pg_staging.get_gold_stats()
        
        if stats:
            print(f"Analytics Dashboard")
            print(f"Total articles: {stats['total_articles']}")
            print(f"Recent articles (last 3 days): {stats['recent_articles']}")
            print(f"Articles by topic:")
            for topic in stats['by_topic'][:5]:  # Top 5 topics
                print(f"  - {topic['topic']}: {topic['count']} articles")
        
        # Push stats to XCom for potential dashboard integration
        kwargs['ti'].xcom_push(key='analytics', value=stats)
        
        return stats
        
    except Exception as e:
        print(f"Error generating analytics: {e}")
        raise
    finally:
        pg_staging.close()


# --- TASK DEFINITIONS ---

setup_task = PythonOperator(
    task_id="setup_services",
    python_callable=setup_services,
    dag=dag,
)

crawl_to_kafka_task = PythonOperator(
    task_id="crawl_and_send_to_kafka",
    python_callable=crawl_and_send_to_kafka,
    dag=dag,
)

kafka_to_bronze_task = PythonOperator(
    task_id="consume_kafka_to_bronze",
    python_callable=consume_from_kafka_to_bronze,
    dag=dag,
)

bronze_to_silver_task = PythonOperator(
    task_id="process_bronze_to_silver",
    python_callable=process_bronze_to_silver,
    dag=dag,
)

silver_to_gold_task = PythonOperator(
    task_id="process_silver_to_gold",
    python_callable=process_silver_to_gold,
    dag=dag,
)

save_final_task = PythonOperator(
    task_id="save_gold_to_final_storage",
    python_callable=save_gold_to_final_storage,
    dag=dag,
)

analytics_task = PythonOperator(
    task_id="generate_analytics",
    python_callable=generate_analytics,
    dag=dag,
)

# --- TASK DEPENDENCIES ---
setup_task >> crawl_to_kafka_task >> kafka_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task >> save_final_task >> analytics_task