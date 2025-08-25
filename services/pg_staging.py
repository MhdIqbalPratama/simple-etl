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
        """Create staging tables for bronze, silver, and gold layers"""
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
                
                # Gold table - final processed data
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pg_gold (
                        id VARCHAR(32) PRIMARY KEY,
                        title TEXT NOT NULL,
                        link TEXT UNIQUE NOT NULL,
                        image TEXT,
                        date TIMESTAMP,
                        topic VARCHAR(100),
                        content TEXT,
                        content_length INTEGER,
                        search_text TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # Create indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_bronze_processed ON pg_bronze (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_processed ON pg_silver (processed)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_topic ON pg_silver (topic)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_silver_date ON pg_silver (date)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_topic ON pg_gold (topic)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_date ON pg_gold (date)")
                
                self.conn.commit()
                print("Staging tables (bronze, silver, gold) created successfully.")
                return True
                
        except Exception as e:
            print(f"Error setting up staging tables: {e}")
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
                    data.append((
                        article.get('id'),
                        article.get('title'),
                        article.get('link'),
                        article.get('image'),
                        str(article.get('date', '')),
                        article.get('topic'),
                        article.get('content')
                    ))
                
                execute_batch(cursor, insert_query, data)
                success_count = cursor.rowcount
                self.conn.commit()
                print(f"Inserted {success_count} records into bronze table")
                
        except Exception as e:
            print(f"Error inserting into bronze table: {e}")
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
                cursor.execute("SELECT * FROM pg_bronze WHERE processed = FALSE")
                bronze_records = cursor.fetchall()
                silver_data = []
                processed_ids = []

                for record in bronze_records:
                    article = {
                        'title': record[1],
                        'content': record[6],
                        'link': record[2],
                        'image': record[3],
                        'date': record[4],
                        'topic': record[5]
                    }
                    cleaned = cleaner.clean_article(article)
                    content_length = len(cleaned['content']) if cleaned['content'] else 0

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
                    processed_ids.append(cleaned['id'])

                # Insert ke silver
                insert_query = """
                    INSERT INTO pg_silver (id, title, link, image, date, topic, content, content_length)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING
                """
                execute_batch(cursor, insert_query, silver_data)

                # Update bronze jadi processed
                if processed_ids:
                    cursor.execute("""
                        UPDATE pg_bronze SET processed = TRUE WHERE id = ANY(%s)
                    """, (processed_ids,))

                self.conn.commit()
                print(f"Processed {len(silver_data)} records from bronze to silver")
                return len(silver_data)

        except Exception as e:
            print(f"Error processing bronze to silver: {e}")
            self.conn.rollback()
            return 0
    
    def process_silver_to_gold(self):
        """Process data from silver to gold (final transformation)"""
        if not self.conn:
            if not self.connect():
                return 0
        
        try:
            with self.conn.cursor() as cursor:
                # Get unprocessed silver records
                cursor.execute("""
                    SELECT * FROM pg_silver 
                    WHERE processed = FALSE
                """)
                silver_records = cursor.fetchall()
                
                if not silver_records:
                    print("No unprocessed silver records found")
                    return 0
                
                # Process each record
                gold_data = []
                processed_ids = []
                
                for record in silver_records:
                    # Create search text (combination of title and content)
                    search_text = f"{record[1]} {record[6]}" if record[1] and record[6] else ""
                    
                    gold_data.append((
                        record[0],  # id
                        record[1],  # title
                        record[2],  # link
                        record[3],  # image
                        record[4],  # date
                        record[5],  # topic
                        record[6],  # content
                        record[7],  # content_length
                        search_text
                    ))
                    processed_ids.append(record[0])
                
                # Insert into gold table
                insert_query = """
                    INSERT INTO pg_gold (id, title, link, image, date, topic, content, content_length, search_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (link) DO NOTHING
                """
                
                execute_batch(cursor, insert_query, gold_data)
                
                # Mark silver records as processed
                if processed_ids:
                    cursor.execute("""
                        UPDATE pg_silver 
                        SET processed = TRUE 
                        WHERE id = ANY(%s)
                    """, (processed_ids,))
                
                self.conn.commit()
                print(f"Processed {len(gold_data)} records from silver to gold")
                return len(gold_data)
                
        except Exception as e:
            print(f"Error processing silver to gold: {e}")
            self.conn.rollback()
            return 0
    
    def get_gold_stats(self):
        """Get statistics from gold table"""
        if not self.conn:
            if not self.connect():
                return None
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Total articles
                cur.execute("SELECT COUNT(*) as total FROM pg_gold")
                total = cur.fetchone()['total']
                
                # Articles by topic
                cur.execute("""
                    SELECT topic, COUNT(*) as count 
                    FROM pg_gold 
                    GROUP BY topic 
                    ORDER BY count DESC
                """)
                by_topic = cur.fetchall()
                
                # Recent articles
                cur.execute("""
                    SELECT COUNT(*) as recent 
                    FROM pg_gold 
                    WHERE date >= NOW() - INTERVAL '3 days'
                """)
                recent = cur.fetchone()['recent']
                
                return {
                    'total_articles': total,
                    'by_topic': list(by_topic),
                    'recent_articles': recent
                }
                
        except Exception as e:
            print(f"Error getting gold stats: {e}")
            return {}
    
    def close(self):
        if self.conn:
            self.conn.close()
            print("PostgreSQL Staging connection closed.")