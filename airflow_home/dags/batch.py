import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import asyncio
from dotenv import load_dotenv

# Import your custom services
from crawler.cnn import CNNCrawler
from services.producer_services import ProducerService
from services.staging_pg import PostgresService
from processor.cleaner import Cleaner

default_args = {
    "owner": "iqbale",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "crawl_cnn_batch_processing",
    default_args=default_args,
    description="Crawl CNN news with batch processing for PostgreSQL",
    schedule_interval="*/5 * * * *",  
    catchup=False,
    max_active_runs=1,
    tags=['cnn', 'kafka', 'crawling', 'batch']
)

def check_services_health(**kwargs):
    """Check if Kafka and PostgreSQL are available"""
    load_dotenv()
    kafka_producer = ProducerService()
    pg_service = PostgresService()
    
    try:
        # Test Kafka connection
        test_msg = {"type": "health_check", "timestamp": datetime.now().isoformat()}
        kafka_success = kafka_producer.send_message(test_msg, key="health_check")
        
        # Test PostgreSQL connection
        pg_success = pg_service.connect()
        
        if kafka_success and pg_success:
            print("All services are healthy")
            return True
        else:
            error_msg = f"Service health check failed - Kafka: {kafka_success}, PostgreSQL: {pg_success}"
            print(f"{error_msg}")
            raise Exception(error_msg)
            
    except Exception as e:
        print(f"Health check failed: {e}")
        raise
    finally:
        kafka_producer.close()
        pg_service.close()

def crawl_articles(**kwargs):
    """Crawl CNN articles and return data"""
    crawler = CNNCrawler()
    cleaner = Cleaner()
    
    try:
        print("Starting CNN crawling process...")
        
        # Configurable page count
        total_pages = int(kwargs.get('dag_run').conf.get('pages', 3)) if kwargs.get('dag_run') and kwargs.get('dag_run').conf else 3
        
        # Crawl articles
        raw_articles = asyncio.run(crawler.crawl_all_contents(total_pages))
        
        if not raw_articles:
            print("No articles found during crawling")
            return {"crawled": 0, "processed": 0}
        
        print(f"Successfully crawled {len(raw_articles)} articles")
        
        # Clean and prepare articles
        processed_articles = []
        for article in raw_articles:
            try:
                # Generate ID and clean data
                article['id'] = cleaner.generate_id(article['link'])
                article['crawled_at'] = datetime.now().isoformat()
                article['source'] = 'cnn_indonesia'
                article['pipeline_stage'] = 'raw'
                
                processed_articles.append(article)
            except Exception as e:
                print(f"Error processing article {article.get('link', 'unknown')}: {e}")
                continue
        
        # Store results in XCom
        result = {
            "crawled": len(raw_articles),
            "processed": len(processed_articles),
            "articles": processed_articles
        }
        
        kwargs['ti'].xcom_push(key='crawl_data', value=result)
        print(f"Crawling completed: {len(processed_articles)} articles processed")
        return result
        
    except Exception as e:
        print(f"Error in crawl_articles: {e}")
        error_info = {"crawled": 0, "processed": 0, "error": str(e)}
        kwargs['ti'].xcom_push(key='crawl_data', value=error_info)
        raise

def batch_insert_bronze(**kwargs):
    """Batch insert articles to PostgreSQL bronze table"""
    ti = kwargs['ti']
    crawl_data = ti.xcom_pull(key='crawl_data', task_ids='crawl_articles_task')
    
    if not crawl_data or not crawl_data.get('articles'):
        print("No articles to insert into bronze")
        return {"inserted": 0}
    
    pg_service = PostgresService()
    
    try:
        if not pg_service.connect():
            raise Exception("Failed to connect to PostgreSQL")
        
        articles = crawl_data['articles']
        inserted_count = pg_service.insert_bronze(articles)
        
        result = {"inserted": inserted_count, "total": len(articles)}
        kwargs['ti'].xcom_push(key='bronze_metrics', value=result)
        
        print(f"Batch inserted {inserted_count}/{len(articles)} articles to bronze table")
        return result
        
    except Exception as e:
        print(f"Error in batch_insert_bronze: {e}")
        error_info = {"inserted": 0, "error": str(e)}
        kwargs['ti'].xcom_push(key='bronze_metrics', value=error_info)
        raise
    finally:
        pg_service.close()

def produce_to_kafka(**kwargs):
    """Produce articles to Kafka raw topic"""
    ti = kwargs['ti']
    crawl_data = ti.xcom_pull(key='crawl_data', task_ids='crawl_articles_task')
    
    if not crawl_data or not crawl_data.get('articles'):
        print("No articles to produce to Kafka")
        return {"produced": 0}
    
    kafka_producer = ProducerService()
    
    try:
        articles = crawl_data['articles']
        
        # Send to Kafka in batches
        batch_size = 10
        total_sent = 0
        
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            sent_count = kafka_producer.send_batch(batch)
            total_sent += sent_count
            print(f"Sent batch {i//batch_size + 1}: {sent_count}/{len(batch)} articles")
        
        result = {"produced": total_sent, "total": len(articles)}
        kwargs['ti'].xcom_push(key='kafka_metrics', value=result)
        
        print(f"Produced {total_sent}/{len(articles)} articles to Kafka")
        return result
        
    except Exception as e:
        print(f"Error in produce_to_kafka: {e}")
        error_info = {"produced": 0, "error": str(e)}
        kwargs['ti'].xcom_push(key='kafka_metrics', value=error_info)
        raise
    finally:
        kafka_producer.close()

def log_batch_metrics(**kwargs):
    """Log metrics for all batch operations"""
    ti = kwargs['ti']
    
    # Pull metrics from all tasks
    crawl_data = ti.xcom_pull(key='crawl_data', task_ids='crawl_articles_task') or {}
    bronze_metrics = ti.xcom_pull(key='bronze_metrics', task_ids='batch_insert_bronze_task') or {}
    kafka_metrics = ti.xcom_pull(key='kafka_metrics', task_ids='produce_to_kafka_task') or {}
    
    print("Batch Processing Metrics:")
    print(f"   Articles Crawled: {crawl_data.get('crawled', 0)}")
    print(f"   Articles Processed: {crawl_data.get('processed', 0)}")
    print(f"   Bronze Inserted: {bronze_metrics.get('inserted', 0)}")
    print(f"   Kafka Produced: {kafka_metrics.get('produced', 0)}")
    
    # Log any errors
    for metrics in [crawl_data, bronze_metrics, kafka_metrics]:
        if 'error' in metrics:
            print(f"   Error: {metrics['error']}")
    
    # Calculate success rates
    crawled = crawl_data.get('crawled', 0)
    if crawled > 0:
        bronze_rate = round((bronze_metrics.get('inserted', 0) / crawled) * 100, 2)
        kafka_rate = round((kafka_metrics.get('produced', 0) / crawled) * 100, 2)
        print(f"   Bronze Success Rate: {bronze_rate}%")
        print(f"   Kafka Success Rate: {kafka_rate}%")

# Task definitions
health_check = PythonOperator(
    task_id="health_check",
    python_callable=check_services_health,
    dag=dag 
)

crawl_articles_task = PythonOperator(
    task_id="crawl_articles_task",
    python_callable=crawl_articles,
    dag=dag
)

# Parallel tasks for bronze insert and kafka produce
batch_insert_bronze_task = PythonOperator(
    task_id="batch_insert_bronze_task",
    python_callable=batch_insert_bronze,
    dag=dag
)

produce_to_kafka_task = PythonOperator(
    task_id="produce_to_kafka_task",
    python_callable=produce_to_kafka,
    dag=dag
)

# SQL task for processing bronze to silver (batch)
process_bronze_to_silver = SQLExecuteQueryOperator(
    task_id="process_bronze_to_silver",
    conn_id="my_postgres",
    sql="sql/process_bronze_to_silver.sql",
    dag=dag
)

# SQL task for processing silver to gold views
process_silver_to_gold = SQLExecuteQueryOperator(
    task_id="process_silver_to_gold",
    conn_id="my_postgres", 
    sql="sql/process_silver_to_gold.sql",
    dag=dag
)

metrics_logging = PythonOperator(
    task_id="log_batch_metrics",
    python_callable=log_batch_metrics,
    dag=dag
)

# Task dependencies
health_check >> crawl_articles_task
crawl_articles_task >> [batch_insert_bronze_task, produce_to_kafka_task]
batch_insert_bronze_task >> process_bronze_to_silver
process_bronze_to_silver >> process_silver_to_gold
[produce_to_kafka_task, process_silver_to_gold] >> metrics_logging