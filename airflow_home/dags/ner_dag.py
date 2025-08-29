import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

from processor.ner import NERProcessor

default_args = {
    "owner": "iqbale",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 21),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    "ner_entity_processing",
    default_args=default_args,
    description="Process articles for Named Entity Recognition",
    schedule_interval="0 */2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=['ner', 'entity_extraction', 'gold_layer']
)

def run_ner_processing(**kwargs):
    """Run NER processing on unprocessed articles"""
    load_dotenv()
    
    try:
        print("Starting NER entity extraction...")
        
        processor = NERProcessor()
        
        # Get batch size from DAG run config or default to 50
        batch_size = 50
        if kwargs.get('dag_run') and kwargs.get('dag_run').conf:
            batch_size = int(kwargs.get('dag_run').conf.get('batch_size', 50))
        
        # Process articles
        result = processor.process_unprocessed_articles(batch_size=batch_size)
        
        # Store results in XCom
        kwargs['ti'].xcom_push(key='ner_results', value=result)
        
        print(f"NER processing completed:")
        print(f"   Articles processed: {result['processed']}")
        print(f"   Entities extracted: {result['entities']}")
        
        if result['processed'] == 0:
            print("No new articles to process")
        
        return result
        
    except Exception as e:
        print(f"Error in NER processing: {e}")
        error_result = {'processed': 0, 'entities': 0, 'error': str(e)}
        kwargs['ti'].xcom_push(key='ner_results', value=error_result)
        raise

def validate_ner_results(**kwargs):
    """Validate NER processing results"""
    ti = kwargs['ti']
    ner_results = ti.xcom_pull(key='ner_results', task_ids='run_ner_processing')
    
    if not ner_results:
        print("No NER results found")
        return False
    
    processed = ner_results.get('processed', 0)
    entities = ner_results.get('entities', 0)
    
    print(f"NER Validation Results:")
    print(f"   Articles processed: {processed}")
    print(f"   Entities extracted: {entities}")
    
    if 'error' in ner_results:
        print(f"   Error: {ner_results['error']}")
        return False
    
    # Basic validation
    if processed > 0 and entities == 0:
        print("Warning: Articles were processed but no entities were extracted")
    elif processed > 0:
        avg_entities = entities / processed
        print(f"   Average entities per article: {avg_entities:.2f}")
        
        if avg_entities < 0.5:
            print("Warning: Very low entity extraction rate")
        else:
            print("Entity extraction rate looks good")
    
    return True

def log_ner_metrics(**kwargs):
    """Log comprehensive NER metrics"""
    ti = kwargs['ti']
    ner_results = ti.xcom_pull(key='ner_results', task_ids='run_ner_processing')
    
    if not ner_results:
        print("No NER metrics to log")
        return
    
    print("NER Processing Metrics Summary:")
    print("=" * 50)
    print(f"Processing Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Articles Processed: {ner_results.get('processed', 0)}")
    print(f"Entities Extracted: {ner_results.get('entities', 0)}")
    
    if 'error' in ner_results:
        print(f"Errors: {ner_results['error']}")
    else:
        print("Status: Completed successfully")
    

# Task definitions
run_ner_processing = PythonOperator(
    task_id="run_ner_processing",
    python_callable=run_ner_processing,
    dag=dag
)

validate_results = PythonOperator(
    task_id="validate_ner_results",
    python_callable=validate_ner_results,
    dag=dag
)

# Update gold views after NER processing
update_gold_views = SQLExecuteQueryOperator(
    task_id="update_gold_views",
    conn_id="my_postgres",
    sql="sql/refresh_gold_views.sql",
    dag=dag
)

log_metrics = PythonOperator(
    task_id="log_ner_metrics",
    python_callable=log_ner_metrics,
    dag=dag
)

# Task dependencies
run_ner_processing >> validate_results >> update_gold_views >> log_metrics