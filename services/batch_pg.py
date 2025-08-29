import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import logging
from typing import List, Dict, Any, Optional
import os
import json
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class PostgresService:
    """Simplified PostgreSQL service without tsvector, focused on stored procedures"""
    
    def __init__(self):
        self.connection_params = {
            "host": os.getenv("PG_HOST", "localhost"),
            "database": os.getenv("PG_DATABASE"), 
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": int(os.getenv("PG_PORT", "5432"))
        }
        self.conn = None
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            self.conn.autocommit = False
            logger.info("PostgreSQL connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def setup_database(self) -> bool:
        """Run initial database setup"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                # Create bronze_lv table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bronze_lv (
                        id VARCHAR(64) PRIMARY KEY,
                        title TEXT,
                        link TEXT UNIQUE NOT NULL,
                        image TEXT,
                        date_raw TEXT,
                        topic TEXT,
                        content TEXT,
                        source VARCHAR(50) DEFAULT 'cnn_indonesia',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Create silver_lv table (simplified, no tsvector)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS silver_lv (
                        id VARCHAR(64) PRIMARY KEY,
                        title TEXT NOT NULL,
                        link TEXT UNIQUE NOT NULL,
                        image TEXT,
                        date TIMESTAMP WITH TIME ZONE,
                        topic VARCHAR(100),
                        content TEXT NOT NULL,
                        content_length INTEGER,
                        source VARCHAR(50) DEFAULT 'cnn_indonesia',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Create indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_lv_processed ON bronze_lv (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_lv_created_at ON bronze_lv (created_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_lv_link ON bronze_lv (link)")
                
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_lv_processed ON silver_lv (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_lv_topic ON silver_lv (topic)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_lv_date ON silver_lv (date DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_lv_link ON silver_lv (link)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_lv_content_length ON silver_lv (content_length)")
                
                self.conn.commit()
                logger.info("Database tables and indexes created successfully")
                return True
                
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def call_bronze_lv_sp(self, articles: List[Dict[str, Any]]) -> Dict[str, int]:
        """Call stored procedure for bronze_lv insertion"""
        if not self.conn:
            if not self.connect():
                return {"inserted": 0, "updated": 0, "errors": 1}
        
        if not articles:
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        try:
            with self.conn.cursor() as cursor:
                # Convert articles to JSON
                articles_json = json.dumps(articles, ensure_ascii=False, default=str)
                
                # Call stored procedure
                cursor.execute(
                    "SELECT inserted_count, updated_count, error_count FROM sp_insert_bronze_lv(%s)",
                    (articles_json,)
                )
                
                result = cursor.fetchone()
                inserted_count, updated_count, error_count = result
                
                self.conn.commit()
                
                logger.info(f"bronze_lv SP completed: {inserted_count} inserted, {updated_count} updated, {error_count} errors")
                return {
                    "inserted": inserted_count,
                    "updated": updated_count,
                    "errors": error_count
                }
                
        except Exception as e:
            logger.error(f"Error calling bronze_lv stored procedure: {e}")
            if self.conn:
                self.conn.rollback()
            return {"inserted": 0, "updated": 0, "errors": 1}
    
    def call_silver_lv_sp(self, articles: List[Dict[str, Any]]) -> Dict[str, int]:
        """Call stored procedure for silver_lv upsert"""
        if not self.conn:
            if not self.connect():
                return {"inserted": 0, "updated": 0, "errors": 1}
        
        if not articles:
            return {"inserted": 0, "updated": 0, "errors": 0}
        
        try:
            with self.conn.cursor() as cursor:
                # Convert articles to JSON
                articles_json = json.dumps(articles, ensure_ascii=False, default=str)
                
                # Call stored procedure
                cursor.execute(
                    "SELECT inserted_count, updated_count, error_count FROM sp_upsert_silver_lv(%s)",
                    (articles_json,)
                )
                
                result = cursor.fetchone()
                inserted_count, updated_count, error_count = result
                
                self.conn.commit()
                
                logger.info(f"silver_lv SP completed: {inserted_count} inserted, {updated_count} updated, {error_count} errors")
                return {
                    "inserted": inserted_count,
                    "updated": updated_count,
                    "errors": error_count
                }
                
        except Exception as e:
            logger.error(f"Error calling silver_lv stored procedure: {e}")
            if self.conn:
                self.conn.rollback()
            return {"inserted": 0, "updated": 0, "errors": 1}
    
    def get_bronze_lv_statistics(self) -> Dict[str, Any]:
        """Get bronze_lv layer statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT * FROM get_bronze_lv_stats()")
                result = cursor.fetchone()
                
                if result:
                    return {
                        "total_records": result[0],
                        "processed_records": result[1],
                        "pending_records": result[2],
                        "latest_created": result[3]
                    }
                
                return {}
                
        except Exception as e:
            logger.error(f"Error getting bronze_lv statistics: {e}")
            return {}
    
    def get_silver_lv_statistics(self) -> Dict[str, Any]:
        """Get silver_lv layer statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT * FROM get_silver_lv_stats()")
                result = cursor.fetchone()
                
                if result:
                    return {
                        "total_records": result[0],
                        "processed_records": result[1],
                        "avg_content_length": result[2],
                        "latest_updated": result[3]
                    }
                
                return {}
                
        except Exception as e:
            logger.error(f"Error getting silver_lv statistics: {e}")
            return {}
    
    def get_gold_analytics(self) -> Dict[str, Any]:
        """Get analytics from gold views"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            analytics = {}
            
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Daily analytics (last 7 days)
                cursor.execute("""
                    SELECT * FROM view_daily_analytics 
                    ORDER BY publish_date DESC 
                    LIMIT 7
                """)
                analytics['daily_trends'] = [dict(row) for row in cursor.fetchall()]
                
                # Topic analytics (top 10)
                cursor.execute("""
                    SELECT * FROM view_topic_analytics 
                    ORDER BY total_articles DESC 
                    LIMIT 10
                """)
                analytics['top_topics'] = [dict(row) for row in cursor.fetchall()]
                
                # Publishing patterns
                cursor.execute("SELECT * FROM view_publishing_patterns ORDER BY publish_hour")
                analytics['hourly_patterns'] = [dict(row) for row in cursor.fetchall()]
                
                # Weekly trends (last 4 weeks)
                cursor.execute("""
                    SELECT * FROM view_weekly_trends 
                    ORDER BY publish_year DESC, publish_week DESC 
                    LIMIT 4
                """)
                analytics['weekly_trends'] = [dict(row) for row in cursor.fetchall()]
                
                # Overall statistics
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_articles,
                        COUNT(DISTINCT topic) as unique_topics,
                        AVG(content_length) as avg_content_length,
                        MIN(date) as earliest_article,
                        MAX(date) as latest_article,
                        COUNT(CASE WHEN has_image = TRUE THEN 1 END) as articles_with_images,
                        COUNT(CASE WHEN content_category = 'Long' THEN 1 END) as long_articles
                    FROM view_gold_lv
                """)
                overall_stats = cursor.fetchone()
                if overall_stats:
                    analytics['overall'] = dict(overall_stats)
                
            return analytics
            
        except Exception as e:
            logger.error(f"Error getting gold analytics: {e}")
            return {}
    
    def search_articles(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Simple text search on articles"""
        if not self.conn:
            if not self.connect():
                return []
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                search_query = """
                    SELECT 
                        id, title, link, content, topic, date, content_length, source
                    FROM view_gold_lv 
                    WHERE 
                        title ILIKE %s OR content ILIKE %s
                    ORDER BY date DESC
                    LIMIT %s
                """
                
                search_term = f"%{query}%"
                cursor.execute(search_query, (search_term, search_term, limit))
                results = cursor.fetchall()
                
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    
    def health_check(self) -> Dict[str, Any]:
        """Check database health and connectivity"""
        try:
            if not self.conn:
                if not self.connect():
                    return {"status": "error", "error": "Cannot connect to database"}
            
            with self.conn.cursor() as cursor:
                # Test query
                cursor.execute("SELECT version()")
                version = cursor.fetchone()[0]
                
                # Check table existence
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('bronze_lv', 'silver_lv')
                """)
                existing_tables = [row[0] for row in cursor.fetchall()]
                
                # Get record counts
                bronze_lv_count = 0
                silver_lv_count = 0
                
                if 'bronze_lv' in existing_tables:
                    cursor.execute("SELECT COUNT(*) FROM bronze_lv")
                    bronze_lv_count = cursor.fetchone()[0]
                
                if 'silver_lv' in existing_tables:
                    cursor.execute("SELECT COUNT(*) FROM silver_lv")
                    silver_lv_count = cursor.fetchone()[0]
                
                return {
                    "status": "healthy",
                    "version": version,
                    "tables_exist": existing_tables,
                    "bronze_lv_count": bronze_lv_count,
                    "silver_lv_count": silver_lv_count
                }
                
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("PostgreSQL connection closed")