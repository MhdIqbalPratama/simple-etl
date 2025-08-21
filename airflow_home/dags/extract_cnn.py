from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from crawler.cnn import CNNCrawler
from dotenv import load_dotenv
import os

default_args = {
    "owner": "iqbale",
    "depends_on_past": False,
    #start date from date now
    "start_date": datetime.now(),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "extract_cnn",
    default_args=default_args,
    description="Extract CNN news data",
    schedule_interval="@daily")


