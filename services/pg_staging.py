import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch
import os
import datetime
from dotenv import load_dotenv
from processor.cleaner import Cleaner

load_dotenv()

class PGStagingService:
    def __init__(self, host=os.getenv("PG_HOST"), database=os.getenv("PG_DATABASE"), 
                 user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"), 
                 port=os.getenv("PG_PORT")):
        self.connection_params = {
            "host": host,
            "database": database,
            "user": user,
            "password": password,
            "port": port
        }
        self.conn = None
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.connection_params)
            print("PostgreSQL Staging connection established.")
            return True
        except Exception as e:
            print(f"An error occurred while connecting to PostgreSQL: {e}")
            return False
    
    def setup_staging_tables(self):
        """Create staging tables for bronze, silver layers and gold view"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                # Bronze table - raw data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pg_bronze (
                        id VARCHAR(32) PRIMARY KEY,
                        title TEXT,
                        link TEXT,
                        image TEXT,
                        date_raw TEXT,
                        topic TEXT,
                        content TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Silver table - cleaned data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pg_silver (
                        id VARCHAR(32) PRIMARY KEY,
                        title TEXT NOT NULL,
                        link TEXT UNIQUE NOT NULL,
                        image TEXT,
                        date TIMESTAMP,
                        topic VARCHAR(100),
                        content TEXT,
                        content_length INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        processed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                # Create indexes for better performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_processed ON pg_bronze (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_created_at ON pg_bronze (created_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processed ON pg_silver (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_topic ON pg_silver (topic)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_date ON pg_silver (date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_created_at ON pg_silver (created_at)")
                
                # Drop the old gold table if it exists
                cursor.execute("DROP TABLE IF EXISTS pg_gold CASCADE")
                
                # Create Gold View (instead of table)
                cursor.execute("DROP VIEW IF EXISTS vw_gold CASCADE")
                
                gold_view_query = """
                CREATE VIEW vw_gold AS
                SELECT 
                    id,
                    title,
                    link,
                    image,
                    date,
                    topic,
                    content,
                    content_length,
                    -- Enhanced search text combining title and content
                    CONCAT(COALESCE(title, ''), ' ', COALESCE(content, '')) as search_text,
                    -- Additional derived fields for analytics
                    CASE 
                        WHEN content_length < 500 THEN 'Short'
                        WHEN content_length < 1500 THEN 'Medium'
                        ELSE 'Long'
                    END as content_category,
                    EXTRACT(HOUR FROM date) as publish_hour,
                    EXTRACT(DOW FROM date) as publish_day_of_week,
                    DATE(date) as publish_date,
                    created_at,
                    -- Content quality indicators
                    CASE WHEN title IS NOT NULL AND LENGTH(TRIM(title)) > 10 THEN TRUE ELSE FALSE END as has_good_title,
                    CASE WHEN content_length > 200 THEN TRUE ELSE FALSE END as has_substantial_content,
                    -- Processing status
                    processed
                FROM pg_silver 
                WHERE title IS NOT NULL 
                  AND content IS NOT NULL
                  AND link IS NOT NULL
                """
                
                cursor.execute(gold_view_query)
                
                self.conn.commit()
                print("Staging tables (bronze, silver) and gold view created successfully.")
                return True
                
        except Exception as e:
            print(f"Error setting up staging tables: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def insert_bronze(self, articles):
        """Insert raw articles into bronze table"""
        if not self.conn:
            if not self.connect():
                return 0
        
        success_count = 0
        try:
            with self.conn.cursor() as cursor:
                insert_query = """
                    INSERT INTO pg_bronze (id, title, link, image, date_raw, topic, content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO NOTHING
                """
                
                data = []
                for article in articles:
                    # Ensure we have required fields
                    if not article.get('link'):
                        continue
                        
                    data.append((
                        article.get('id'),
                        article.get('title'),
                        article.get('link'),
                        article.get('image'),
                        str(article.get('date', '')),
                        article.get('topic'),
                        article.get('content')
                    ))
                
                if data:
                    execute_batch(cursor, insert_query, data)
                    success_count = cursor.rowcount
                    self.conn.commit()
                    print(f"Inserted {success_count} records into bronze table")
                
        except Exception as e:
            print(f"Error inserting into bronze table: {e}")
            if self.conn:
                self.conn.rollback()
        
        return success_count
    
    def process_bronze_to_silver(self):
        """Process data from bronze to silver (cleaning)"""
        if not self.conn:
            if not self.connect():
                return 0
        
        try:
            cleaner = Cleaner()
            with self.conn.cursor() as cursor:
                # Get unprocessed bronze records
                cursor.execute("""
                    SELECT id, title, link, image, date_raw, topic, content 
                    FROM pg_bronze 
                    WHERE processed = FALSE
                    ORDER BY created_at
                """)
                bronze_records = cursor.fetchall()
                
                if not bronze_records:
                    return 0

                silver_data = []
                processed_ids = []

                for record in bronze_records:
                    try:
                        article = {
                            'title': record[1],
                            'content': record[6],
                            'link': record[2],
                            'image': record[3],
                            'date': record[4],
                            'topic': record[5]
                        }
                        
                        cleaned = cleaner.clean_article(article)
                        
                        # Skip if cleaning failed
                        if not cleaned or not cleaned.get('link'):
                            continue
                            
                        content_length = len(cleaned.get('content', '')) if cleaned.get('content') else 0

                        silver_data.append((
                            cleaned['id'],
                            cleaned['title'],
                            cleaned['link'],
                            cleaned['image'],
                            cleaned['date'],
                            cleaned['topic'],
                            cleaned['content'],
                            content_length
                        ))
                        processed_ids.append(record[0])  # Original bronze ID
                        
                    except Exception as e:
                        print(f"Error processing article {record[0]}: {e}")
                        continue

                # Insert into silver table
                if silver_data:
                    insert_query = """
                        INSERT INTO pg_silver (id, title, link, image, date, topic, content, content_length)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (link) DO UPDATE SET
                            title = EXCLUDED.title,
                            image = EXCLUDED.image,
                            date = EXCLUDED.date,
                            topic = EXCLUDED.topic,
                            content = EXCLUDED.content,
                            content_length = EXCLUDED.content_length,
                            processed = FALSE
                    """
                    execute_batch(cursor, insert_query, silver_data)

                # Mark bronze records as processed
                if processed_ids:
                    cursor.execute("""
                        UPDATE pg_bronze SET processed = TRUE 
                        WHERE id = ANY(%s)
                    """, (processed_ids,))

                self.conn.commit()
                print(f"Processed {len(silver_data)} records from bronze to silver")
                return len(silver_data)

        except Exception as e:
            print(f"Error processing bronze to silver: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def process_silver_to_gold(self):
        """
        Mark silver records as processed - Gold is now a view, not a physical table
        This method updates the processed flag in silver table
        """
        if not self.conn:
            if not self.connect():
                return 0
        
        try:
            with self.conn.cursor() as cursor:
                # Mark unprocessed silver records as processed
                cursor.execute("""
                    UPDATE pg_silver 
                    SET processed = TRUE 
                    WHERE processed = FALSE
                      AND title IS NOT NULL 
                      AND content IS NOT NULL 
                      AND link IS NOT NULL
                """)
                
                processed_count = cursor.rowcount
                self.conn.commit()
                
                if processed_count > 0:
                    print(f"Marked {processed_count} silver records as processed (available in gold view)")
                
                return processed_count
                
        except Exception as e:
            print(f"Error processing silver records: {e}")
            if self.conn:
                self.conn.rollback()
            return 0
    
    def get_gold_stats(self):
        """Get statistics from gold view"""
        if not self.conn:
            if not self.connect():
                return None
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Total articles in gold view
                cur.execute("SELECT COUNT(*) as total FROM vw_gold")
                total = cur.fetchone()['total']
                
                # Articles by topic
                cur.execute("""
                    SELECT topic, COUNT(*) as count 
                    FROM vw_gold 
                    GROUP BY topic 
                    ORDER BY count DESC
                """)
                by_topic = cur.fetchall()
                
                # Recent articles (last 3 days)
                cur.execute("""
                    SELECT COUNT(*) as recent 
                    FROM vw_gold 
                    WHERE date >= NOW() - INTERVAL '3 days'
                """)
                recent = cur.fetchone()['recent']
                
                # Quality metrics
                cur.execute("""
                    SELECT 
                        COUNT(CASE WHEN has_good_title THEN 1 END) as good_titles,
                        COUNT(CASE WHEN has_substantial_content THEN 1 END) as substantial_content,
                        AVG(content_length) as avg_content_length
                    FROM vw_gold
                """)
                quality_stats = cur.fetchone()
                
                return {
                    'total_articles': total,
                    'by_topic': list(by_topic),
                    'recent_articles': recent,
                    'good_titles': quality_stats['good_titles'],
                    'substantial_content': quality_stats['substantial_content'],
                    'avg_content_length': float(quality_stats['avg_content_length']) if quality_stats['avg_content_length'] else 0
                }
                
        except Exception as e:
            print(f"Error getting gold stats: {e}")
            return {}
    
    def get_bronze_stats(self):
        """Get bronze layer statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed THEN 1 END) as processed,
                        COUNT(CASE WHEN NOT processed THEN 1 END) as pending
                    FROM pg_bronze
                """)
                stats = cur.fetchone()
                return dict(stats)
                
        except Exception as e:
            print(f"Error getting bronze stats: {e}")
            return {}
    
    def get_silver_stats(self):
        """Get silver layer statistics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN processed THEN 1 END) as processed,
                        COUNT(CASE WHEN NOT processed THEN 1 END) as pending,
                        AVG(content_length) as avg_content_length
                    FROM pg_silver
                """)
                stats = cur.fetchone()
                return dict(stats)
                
        except Exception as e:
            print(f"Error getting silver stats: {e}")
            return {}
    
    def cleanup_old_data(self, days_to_keep=30):
        """Clean up old data from bronze and silver tables"""
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                # Clean up old processed bronze records
                cursor.execute("""
                    DELETE FROM pg_bronze 
                    WHERE processed = TRUE 
                      AND created_at < NOW() - INTERVAL '%s days'
                """, (days_to_keep,))
                
                bronze_deleted = cursor.rowcount
                
                # Clean up old processed silver records (keep more silver data)
                cursor.execute("""
                    DELETE FROM pg_silver 
                    WHERE processed = TRUE 
                      AND created_at < NOW() - INTERVAL '%s days'
                """, (days_to_keep * 2,))  # Keep silver data twice as long
                
                silver_deleted = cursor.rowcount
                
                self.conn.commit()
                print(f"Cleanup completed: {bronze_deleted} bronze records, {silver_deleted} silver records deleted")
                
                return True
                
        except Exception as e:
            print(f"Error during cleanup: {e}")
            if self.conn:
                self.conn.rollback()
            return False
    
    def get_pipeline_health(self):
        """Get overall pipeline health metrics"""
        if not self.conn:
            if not self.connect():
                return {}
        
        try:
            bronze_stats = self.get_bronze_stats()
            silver_stats = self.get_silver_stats()
            gold_stats = self.get_gold_stats()
            
            # Calculate health metrics
            bronze_total = bronze_stats.get('total', 0)
            silver_total = silver_stats.get('total', 0)
            gold_total = gold_stats.get('total_articles', 0)
            
            # Processing efficiency (bronze to gold)
            processing_efficiency = (gold_total / bronze_total * 100) if bronze_total > 0 else 0
            
            # Data quality score
            good_titles = gold_stats.get('good_titles', 0)
            substantial_content = gold_stats.get('substantial_content', 0)
            quality_score = ((good_titles + substantial_content) / (gold_total * 2) * 100) if gold_total > 0 else 0
            
            return {
                'bronze': bronze_stats,
                'silver': silver_stats,
                'gold': gold_stats,
                'processing_efficiency': round(processing_efficiency, 2),
                'quality_score': round(quality_score, 2),
                'pipeline_status': 'healthy' if processing_efficiency > 70 and quality_score > 60 else 'needs_attention'
            }
            
        except Exception as e:
            print(f"Error getting pipeline health: {e}")
            return {}
    
    def close(self):
        if self.conn:
            self.conn.close()
            print("PostgreSQL Staging connection closed.")
        else:
            print("No active PostgreSQL Staging connection to close.")