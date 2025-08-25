import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
from dotenv import load_dotenv

# Import your custom services
from crawler.cnn import CNNCrawler
from services.producer_services import ProducerService

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
    "crawl_cnn_to_kafka",
    default_args=default_args,
    description="Crawl CNN news and send to Kafka",
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=['cnn', 'kafka', 'crawling']
)

def check_kafka_connection(**kwargs):
    """Check if Kafka is available before crawling"""
    load_dotenv()
    kafka_producer = ProducerService()
    
    try:
        # Test connection by sending a test message
        test_msg = {"type": "health_check", "timestamp": datetime.now().isoformat()}
        success = kafka_producer.send_message(test_msg, key="health_check")
        
        if success:
            print("Kafka connection successful")
            return True
        else:
            print("Kafka connection failed")
            raise Exception("Kafka connection failed")
            
    except Exception as e:
        print(f"Kafka health check failed: {e}")
        raise
    finally:
        kafka_producer.close()

def crawl_and_produce(**kwargs):
    """Crawl CNN news and produce to Kafka"""
    crawler = CNNCrawler()
    kafka_producer = ProducerService()
    
    try:
        print("🕷️ Starting CNN crawling process...")
        
        # Configurable page count
        total_pages = int(kwargs.get('dag_run').conf.get('pages', 3)) if kwargs.get('dag_run') and kwargs.get('dag_run').conf else 3
        
        # Crawl articles
        articles = asyncio.run(crawler.crawl_all_contents(total_pages))
        
        if not articles:
            print("No articles found during crawling")
            return {"crawled": 0, "sent_to_kafka": 0}
        
        print(f"Successfully crawled {len(articles)} articles")
        
        # Add metadata to each article
        for article in articles:
            article['crawled_at'] = datetime.now().isoformat()
            article['source'] = 'cnn_indonesia'
            article['pipeline_stage'] = 'raw'
        
        # Send to Kafka in batches
        batch_size = 10
        total_sent = 0
        
        for i in range(0, len(articles), batch_size):
            batch = articles[i:i + batch_size]
            sent_count = kafka_producer.send_batch(batch)
            total_sent += sent_count
            print(f"Sent batch {i//batch_size + 1}: {sent_count}/{len(batch)} articles")
        
        # Push metrics to XCom
        result = {
            "crawled": len(articles),
            "sent_to_kafka": total_sent,
            "success_rate": round((total_sent / len(articles)) * 100, 2) if articles else 0
        }
        
        kwargs['ti'].xcom_push(key='crawl_metrics', value=result)
        
        print(f"Crawling completed: {total_sent}/{len(articles)} articles sent to Kafka")
        return result
        
    except Exception as e:
        print(f"Error in crawl_and_produce: {e}")
        # Push error info to XCom
        error_info = {
            "error": str(e),
            "crawled": 0,
            "sent_to_kafka": 0,
            "success_rate": 0
        }
        kwargs['ti'].xcom_push(key='crawl_metrics', value=error_info)
        raise
        
    finally:
        kafka_producer.close()

def log_metrics(**kwargs):
    """Log metrics for monitoring"""
    ti = kwargs['ti']
    metrics = ti.xcom_pull(key='crawl_metrics', task_ids='crawl_and_produce_task')
    
    if metrics:
        print("Crawling Metrics:")
        print(f"   Articles Crawled: {metrics.get('crawled', 0)}")
        print(f"   Sent to Kafka: {metrics.get('sent_to_kafka', 0)}")
        print(f"   Success Rate: {metrics.get('success_rate', 0)}%")
        
        # Log any errors
        if 'error' in metrics:
            print(f"   Error: {metrics['error']}")
        
        # You could extend this to send metrics to monitoring systems
        # like Prometheus, Grafana, or logging services
        
    else:
        print("No metrics available")

# Task definitions
kafka_health_check = PythonOperator(
    task_id="kafka_health_check",
    python_callable=check_kafka_connection,
    dag=dag 
)

crawl_and_produce_task = PythonOperator(
    task_id="crawl_and_produce_task", 
    python_callable=crawl_and_produce,
    dag=dag
)

metrics_logging = PythonOperator(
    task_id="log_crawl_metrics",
    python_callable=log_metrics,
    dag=dag
)

# Task dependencies
kafka_health_check >> crawl_and_produce_task >> metrics_logging