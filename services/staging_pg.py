import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import logging
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class PostgresService:
    
    def __init__(self):
        self.connection_params = {
            "host": os.getenv("PG_HOST"),
            "database": os.getenv("PG_DATABASE"),
            "user": os.getenv("PG_USER"),
            "password": os.getenv("PG_PASSWORD"),
            "port": int(os.getenv("PG_PORT"))
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
                # Create schemas
                # cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
                
                # Create bronze table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bronze (
                        id VARCHAR(64) PRIMARY KEY,
                        title TEXT,
                        link TEXT UNIQUE,
                        image TEXT,
                        date_raw TEXT,
                        topic TEXT,
                        content TEXT,
                        source VARCHAR(50) DEFAULT 'cnn_indonesia',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Create silver table with enhanced fields
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS silver (
                        id VARCHAR(64) PRIMARY KEY,
                        title TEXT NOT NULL,
                        link TEXT UNIQUE NOT NULL,
                        image TEXT,
                        date TIMESTAMP WITH TIME ZONE,
                        topic VARCHAR(100),
                        content TEXT NOT NULL,
                        content_length INTEGER,
                        search_vector tsvector,
                        source VARCHAR(50) DEFAULT 'cnn_indonesia',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Create gold view
                cursor.execute("DROP VIEW IF EXISTS view_gold CASCADE")
                cursor.execute("""
                    CREATE VIEW view_gold AS
                    SELECT 
                        id,
                        title,
                        link,
                        image,
                        date,
                        topic,
                        content,
                        content_length,
                        search_vector,
                        source,
                        -- Enhanced analytics fields
                        CASE 
                            WHEN content_length < 500 THEN 'Short'
                            WHEN content_length < 1500 THEN 'Medium'
                            ELSE 'Long'
                        END as content_category,
                        EXTRACT(HOUR FROM date) as publish_hour,
                        EXTRACT(DOW FROM date) as publish_day_of_week,
                        DATE(date) as publish_date,
                        -- Quality indicators
                        CASE WHEN title IS NOT NULL AND LENGTH(title) > 10 THEN TRUE ELSE FALSE END as has_good_title,
                        CASE WHEN content_length > 200 THEN TRUE ELSE FALSE END as has_substantial_content,
                        -- Search text
                        CONCAT(title, ' ', content) as search_text,
                        created_at,
                        updated_at
                    FROM silver 
                    WHERE processed = TRUE
                      AND title IS NOT NULL 
                      AND content IS NOT NULL
                      AND date IS NOT NULL
                """)
                
                # Create indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_processed ON bronze (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_created_at ON bronze (created_at DESC)")
                
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processed ON silver (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_topic ON silver (topic)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_date ON silver (date DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_link ON silver (link)")
                
                # Create GIN index for full-text search
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_silver_search_vector 
                    ON silver USING GIN (search_vector)
                """)
                
                # Create trigger for tsvector updates
                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_search_vector()
                    RETURNS trigger AS $$
                    BEGIN
                        NEW.search_vector := 
                            setweight(to_tsvector('indonesian', COALESCE(NEW.title, '')), 'A') ||
                            setweight(to_tsvector('indonesian', COALESCE(NEW.content, '')), 'B') ||
                            setweight(to_tsvector('indonesian', COALESCE(NEW.topic, '')), 'C');
                        NEW.updated_at := CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END
                    $$ LANGUAGE plpgsql
                """)
                
                cursor.execute("DROP TRIGGER IF EXISTS trig_update_search_vector ON silver")
                cursor.execute("""
                    CREATE TRIGGER trig_update_search_vector
                    BEFORE INSERT OR UPDATE ON silver
                    FOR EACH ROW EXECUTE FUNCTION update_search_vector()
                """)
                
                self.conn.commit()
                logger.info("Database setup completed successfully")
                return True
                
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def insert_bronze(self, articles: List[Dict[str, Any]]) -> int:
        """Insert raw articles into bronze table"""
        if not self.conn:
            if not self.connect():
                return 0
        
        if not articles:
            return 0
        
        try:
            with self.conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO bronze (id, title, link, image, date_raw, topic, content, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO UPDATE SET
                        title = EXCLUDED.title,
                        image = EXCLUDED.image,
                        date_raw = EXCLUDED.date_raw,
                        topic = EXCLUDED.topic,
                        content = EXCLUDED.content,
                        source = EXCLUDED.source,
                        created_at = CURRENT_TIMESTAMP,
                        processed = FALSE
                """
                
                data = []
                for article in articles:
                    data.append((
                        article.get('id'),
                        article.get('title'),
                        article.get('link'),
                        article.get('image'),
                        str(article.get('date', '')),
                        article.get('topic'),
                        article.get('content'),
                        article.get('source', 'cnn_indonesia')
                    ))
                
                execute_batch(cursor, insert_query, data, page_size=100)
                affected_rows = cursor.rowcount
                self.conn.commit()
                
                logger.info(f"Inserted/updated {affected_rows} records in bronze table")
                return affected_rows
                
        except Exception as e:
            logger.error(f"Error inserting into bronze table: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def upsert_silver(self, articles: List[Dict[str, Any]]) -> int:
        """Upsert cleaned articles into silver table"""
        if not self.conn:
            if not self.connect():
                return 0
        
        if not articles:
            return 0
        
        try:
            with self.conn.cursor() as cursor:
                upsert_query = """
                    INSERT INTO silver (id, title, link, image, date, topic, content, content_length, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO UPDATE SET
                        title = EXCLUDED.title,
                        image = EXCLUDED.image,
                        date = EXCLUDED.date,
                        topic = EXCLUDED.topic,
                        content = EXCLUDED.content,
                        content_length = EXCLUDED.content_length,
                        source = EXCLUDED.source,
                        updated_at = CURRENT_TIMESTAMP,
                        processed = TRUE
                """
                
                data = []
                for article in articles:
                    content_length = len(article.get('content', ''))
                    data.append((
                        article.get('id'),
                        article.get('title'),
                        article.get('link'),
                        article.get('image'),
                        article.get('date'),
                        article.get('topic'),
                        article.get('content'),
                        content_length,
                        article.get('source', 'cnn_indonesia')
                    ))
                
                execute_batch(cursor, upsert_query, data, page_size=100)
                affected_rows = cursor.rowcount
                self.conn.commit()
                
                logger.info(f"Upserted {affected_rows} records in silver table")
                return affected_rows
                
        except Exception as e:
            logger.error(f"Error upserting into silver table: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def full_text_search(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Perform full-text search on silver table"""
        if not self.conn:
            if not self.connect():
                return []
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                search_query = """
                    SELECT 
                        id, title, link, content, topic, date, content_length,
                        ts_rank(search_vector, plainto_tsquery('indonesian', %s)) as rank
                    FROM view_gold 
                    WHERE search_vector @@ plainto_tsquery('indonesian', %s)
                    ORDER BY rank DESC, date DESC
                    LIMIT %s
                """
                
                cursor.execute(search_query, (query, query, limit))
                results = cursor.fetchall()
                
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Full-text search error: {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                stats = {}
                
                # Bronze stats
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                        COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending
                    FROM bronze
                """)
                stats['bronze'] = dict(cursor.fetchone())
                
                # Silver stats
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        AVG(content_length) as avg_content_length,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed
                    FROM silver
                """)
                stats['silver'] = dict(cursor.fetchone())
                
                # Gold stats
                cursor.execute("SELECT COUNT(*) as total FROM vw_gold")
                stats['gold'] = {'total': cursor.fetchone()['total']}
                
                # Topic distribution
                cursor.execute("""
                    SELECT topic, COUNT(*) as count
                    FROM vw_gold
                    GROUP BY topic
                    ORDER BY count DESC
                    LIMIT 10
                """)
                stats['topics'] = [dict(row) for row in cursor.fetchall()]
                
                return stats
                
        except Exception as e:
            logger.error(f"Error getting statistics: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")