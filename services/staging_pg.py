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
        """Run initial database setup for medallion architecture"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                # Create bronze table (raw data archive)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS bronze (
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
                
                # Create silver table (cleaned and processed data)
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
                        source VARCHAR(50) DEFAULT 'cnn_indonesia',
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Create gold_entities table for NER results
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS gold_entities (
                        id SERIAL PRIMARY KEY,
                        article_id VARCHAR(64) NOT NULL,
                        entity_text TEXT NOT NULL,
                        entity_type VARCHAR(20) NOT NULL,
                        confidence_score FLOAT DEFAULT 0.0,
                        start_position INTEGER,
                        end_position INTEGER,
                        processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (article_id) REFERENCES silver(id) ON DELETE CASCADE
                    )
                """)
                
                # Create indexes for bronze table
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_processed ON bronze (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_created_at ON bronze (created_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_link ON bronze (link)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_source ON bronze (source)")
                
                # Create indexes for silver table
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processed ON silver (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_topic ON silver (topic)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_date ON silver (date DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_link ON silver (link)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_content_length ON silver (content_length)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_source ON silver (source)")
                
                # Create indexes for gold_entities table
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_entities_article_id ON gold_entities (article_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_entities_type ON gold_entities (entity_type)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_entities_text ON gold_entities (entity_text)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_entities_processed_at ON gold_entities (processed_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_entities_confidence ON gold_entities (confidence_score)")
                
                # Create trigger for silver table updated_at
                cursor.execute("""
                    CREATE OR REPLACE FUNCTION update_modified_time()
                    RETURNS TRIGGER AS $
                    BEGIN
                        NEW.updated_at = CURRENT_TIMESTAMP;
                        RETURN NEW;
                    END;
                    $ LANGUAGE plpgsql
                """)
                
                cursor.execute("DROP TRIGGER IF EXISTS trig_update_silver_modified_time ON silver")
                cursor.execute("""
                    CREATE TRIGGER trig_update_silver_modified_time
                    BEFORE UPDATE ON silver
                    FOR EACH ROW EXECUTE FUNCTION update_modified_time()
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
        """Batch insert raw articles into bronze table"""
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
                
                logger.info(f"Batch inserted/updated {affected_rows} records in bronze table")
                return affected_rows
                
        except Exception as e:
            logger.error(f"Error inserting into bronze table: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def upsert_silver(self, articles: List[Dict[str, Any]]) -> int:
        """Batch upsert cleaned articles into silver table"""
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
                
                logger.info(f"Batch upserted {affected_rows} records in silver table")
                return affected_rows
                
        except Exception as e:
            logger.error(f"Error upserting into silver table: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def insert_entities(self, entities: List[Dict[str, Any]]) -> int:
        """Batch insert NER entities into gold_entities table"""
        if not self.conn:
            if not self.connect():
                return 0
        
        if not entities:
            return 0
        
        try:
            with self.conn.cursor() as cursor:
                # First, delete existing entities for these articles
                article_ids = list(set([e.get('article_id') for e in entities if e.get('article_id')]))
                if article_ids:
                    cursor.execute(
                        "DELETE FROM gold_entities WHERE article_id = ANY(%s)",
                        (article_ids,)
                    )
                
                # Insert new entities
                insert_query = """
                    INSERT INTO gold_entities (
                        article_id, entity_text, entity_type, confidence_score, 
                        start_position, end_position
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                
                data = []
                for entity in entities:
                    data.append((
                        entity.get('article_id'),
                        entity.get('entity_text'),
                        entity.get('entity_type'),
                        float(entity.get('confidence_score', 0.0)),
                        float(entity.get('start_position')),
                        float(entity.get('end_position'))
                    ))
                
                execute_batch(cursor, insert_query, data, page_size=100)
                affected_rows = cursor.rowcount
                self.conn.commit()
                
                logger.info(f"Batch inserted {affected_rows} entities into gold_entities table")
                return affected_rows
                
        except Exception as e:
            logger.error(f"Error inserting entities: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def get_unprocessed_articles_for_ner(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get articles from silver that need NER processing"""
        if not self.conn:
            if not self.connect():
                return []
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    SELECT s.id, s.title, s.content, s.topic, s.date
                    FROM silver s
                    LEFT JOIN gold_entities ge ON s.id = ge.article_id
                    WHERE s.processed = TRUE 
                      AND s.content IS NOT NULL 
                      AND LENGTH(s.content) > 100
                      AND ge.article_id IS NULL  -- Not yet processed for NER
                    ORDER BY s.created_at DESC
                    LIMIT %s
                """
                
                cursor.execute(query, (limit,))
                results = cursor.fetchall()
                
                return [dict(row) for row in results]
                
        except Exception as e:
            logger.error(f"Error getting unprocessed articles: {e}")
            return []
    
    def get_bronze_statistics(self) -> Dict[str, Any]:
        """Get bronze table statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                        COUNT(CASE WHEN processed = FALSE THEN 1 END) as pending,
                        MIN(created_at) as oldest_record,
                        MAX(created_at) as newest_record,
                        COUNT(DISTINCT topic) as unique_topics,
                        COUNT(DISTINCT source) as unique_sources
                    FROM bronze
                """)
                
                return dict(cursor.fetchone())
                
        except Exception as e:
            logger.error(f"Error getting bronze statistics: {e}")
            return {}
    
    def get_silver_statistics(self) -> Dict[str, Any]:
        """Get silver table statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed,
                        AVG(content_length) as avg_content_length,
                        MIN(date) as earliest_date,
                        MAX(date) as latest_date,
                        COUNT(DISTINCT topic) as unique_topics
                    FROM silver
                """)
                
                return dict(cursor.fetchone())
                
        except Exception as e:
            logger.error(f"Error getting silver statistics: {e}")
            return {}
    
    def get_entities_statistics(self) -> Dict[str, Any]:
        """Get gold entities statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        entity_type,
                        COUNT(*) as count,
                        COUNT(DISTINCT entity_text) as unique_entities,
                        AVG(confidence_score) as avg_confidence,
                        COUNT(DISTINCT article_id) as articles_with_entities
                    FROM gold_entities 
                    GROUP BY entity_type
                    ORDER BY count DESC
                """)
                
                results = cursor.fetchall()
                return {row['entity_type']: dict(row) for row in results}
                
        except Exception as e:
            logger.error(f"Error getting entities statistics: {e}")
            return {}
    
    def get_comprehensive_statistics(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        return {
            'bronze': self.get_bronze_statistics(),
            'silver': self.get_silver_statistics(),
            'entities': self.get_entities_statistics(),
            'timestamp': 'CURRENT_TIMESTAMP'
        }
    
    def execute_sql_file(self, sql_file_path: str) -> bool:
        """Execute SQL file (for stored procedures)"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with open(sql_file_path, 'r', encoding='utf-8') as file:
                sql_content = file.read()
            
            with self.conn.cursor() as cursor:
                cursor.execute(sql_content)
                self.conn.commit()
                
            logger.info(f"Successfully executed SQL file: {sql_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error executing SQL file {sql_file_path}: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("PostgreSQL connection closed")