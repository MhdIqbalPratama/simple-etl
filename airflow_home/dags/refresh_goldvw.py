import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from services.staging_pg import PostgresService

load_dotenv()

default_args = {
    "owner": "etl_pipeline",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 21),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
}

dag = DAG(
    "refresh_materialized_views",
    default_args=default_args,
    description="Refresh materialized views and update statistics",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    max_active_runs=1,
    tags=['maintenance', 'views', 'statistics']
)

def refresh_search_vectors(**kwargs):
    """Refresh tsvector search indexes"""
    try:
        pg_service = PostgresService()
        if not pg_service.connect():
            raise Exception("Cannot connect to PostgreSQL")
        
        with pg_service.conn.cursor() as cursor:
            # Update search vectors for unprocessed records
            cursor.execute("""
                UPDATE silver 
                SET search_vector = 
                    setweight(to_tsvector('indonesian', COALESCE(title, '')), 'A') ||
                    setweight(to_tsvector('indonesian', COALESCE(content, '')), 'B') ||
                    setweight(to_tsvector('indonesian', COALESCE(topic, '')), 'C'),
                    updated_at = CURRENT_TIMESTAMP
                WHERE search_vector IS NULL 
                   OR updated_at < created_at
            """)
            
            updated_rows = cursor.rowcount
            pg_service.conn.commit()
            
            print(f"✅ Updated {updated_rows} search vectors")
            
        pg_service.close()
        return updated_rows
        
    except Exception as e:
        print(f"❌ Error refreshing search vectors: {e}")
        raise

def update_statistics(**kwargs):
    """Update table statistics and analyze performance"""
    try:
        pg_service = PostgresService()
        if not pg_service.connect():
            raise Exception("Cannot connect to PostgreSQL")
        
        with pg_service.conn.cursor() as cursor:
            # Analyze tables
            tables = ['bronze', 'silver']
            for table in tables:
                cursor.execute(f"ANALYZE {table}")
                print(f"✅ Analyzed table: {table}")
            
            # Get current statistics
            stats = pg_service.get_statistics()
            print(f"📊 Current Statistics: {stats}")
            
            # Clean up old bronze records (older than 7 days)
            cursor.execute("""
                DELETE FROM bronze 
                WHERE processed = TRUE 
                  AND created_at < CURRENT_TIMESTAMP - INTERVAL '7 days'
            """)
            
            deleted_rows = cursor.rowcount
            pg_service.conn.commit()
            
            print(f"🧹 Cleaned up {deleted_rows} old bronze records")
            
        pg_service.close()
        return stats
        
    except Exception as e:
        print(f"❌ Error updating statistics: {e}")
        raise

def vacuum_tables(**kwargs):
    """Vacuum tables to reclaim space"""
    try:
        pg_service = PostgresService()
        if not pg_service.connect():
            raise Exception("Cannot connect to PostgreSQL")
        
        # Set autocommit for VACUUM
        pg_service.conn.autocommit = True
        
        with pg_service.conn.cursor() as cursor:
            tables = ['bronze', 'silver']
            for table in tables:
                cursor.execute(f"VACUUM ANALYZE {table}")
                print(f"🧹 Vacuumed table: {table}")
        
        pg_service.close()
        print("✅ All tables vacuumed successfully")
        
    except Exception as e:
        print(f"❌ Error vacuuming tables: {e}")
        raise

# Task definitions
refresh_vectors = PythonOperator(
    task_id="refresh_search_vectors",
    python_callable=refresh_search_vectors,
    dag=dag
)

update_stats = PythonOperator(
    task_id="update_statistics",
    python_callable=update_statistics,
    dag=dag
)

vacuum_task = PythonOperator(
    task_id="vacuum_tables",
    python_callable=vacuum_tables,
    dag=dag
)

# Task dependencies
refresh_vectors >> update_stats >> vacuum_task