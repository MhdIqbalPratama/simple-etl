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
from services.es_services import ESService
from services.pg_service import PGService
from processor.cleaner import Cleaner  # moved import to top (better practice)


default_args = {
    "owner": "iqbale",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 21),  # use fixed date, not dynamic now()
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG(
    "extract_cnn",
    default_args=default_args,
    description="Extract CNN news data",
    schedule_interval="@daily",
    catchup=False,  # don't backfill
)

# --- FUNCTIONS ---

def set_db_services(**kwargs):
    load_dotenv()
    es_service = ESService()
    pg_service = PGService(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT"),
    )
    es_service.setup_index()
    pg_service.setup_table()
    print(" Database services setup completed.")


def crawl_cnn_news(**kwargs):
    crawler = CNNCrawler()
    total_page = 10  # configurable
    articles = asyncio.run(crawler.crawl_all_contents(total_page))

    if not articles:
        print(" No articles found.")
        return []

    print(f" Total articles crawled: {len(articles)}")

    # Push to XCom
    return articles


def clean_news(**kwargs):
    ti = kwargs["ti"]
    raw_articles = ti.xcom_pull(task_ids="crawl_cnn_news")
    if not raw_articles:
        print("⚠️ No raw articles from crawler.")
        return []

    cleaner = Cleaner()
    clean_articles = cleaner.clean_all_articles(raw_articles)

    print(f" Total articles after cleaning: {len(clean_articles)}")
    return clean_articles


def save_news_to_storage(**kwargs):
    load_dotenv()
    es_service = ESService()
    pg_service = PGService(
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        port=os.getenv("PG_PORT"),
    )

    ti = kwargs["ti"]
    clean_articles = ti.xcom_pull(task_ids="clean_news")

    if not clean_articles:
        print(" No articles to save.")
        return

    es_success = es_service.save_bulk(clean_articles)
    pg_success = pg_service.save_bulk(clean_articles)

    print(f" Total articles saved to Elasticsearch: {es_success}")
    print(f" Total articles saved to PostgreSQL: {pg_success}")


# --- TASK DEFINITIONS ---
setup_db_task = PythonOperator(
    task_id="setup_db_services",
    python_callable=set_db_services,
    dag=dag,
)

crawl_task = PythonOperator(
    task_id="crawl_cnn_news",
    python_callable=crawl_cnn_news,
    dag=dag,
)

clean_task = PythonOperator(
    task_id="clean_news",
    python_callable=clean_news,
    dag=dag,
)

save_task = PythonOperator(
    task_id="save_news_to_storage",
    python_callable=save_news_to_storage,
    dag=dag,
)


setup_db_task >> [crawl_task, clean_task ]>> save_task
    