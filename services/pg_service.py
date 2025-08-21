import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import os

class PGService:
    def __init__(self, host=os.getenv("PG_HOST"), database=os.getenv("PG_DATABASE"), user=os.getenv("PG_USER"), password=os.getenv("PG_PASSWORD"), port=os.getenv("PG_PORT")):
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
            print("PostgreSQL connection established.")
            return True
        except Exception as e:
            print(f"An error occurred while connecting to PostgreSQL: {e}")
            return False
    
    def setup_table(self):
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS articles (
                        id VARCHAR(32) PRIMARY KEY,
                        title TEXT NOT NULL,
                        link TEXT UNIQUE NOT NULL,
                        image TEXT,
                        date TIMESTAMP,
                        topic VARCHAR(100),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)


                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_articles_topic ON articles (topic);
                """)
                cursor.execute("""
                    CREATE INDEX IF NOT EXISTS idx_articles_date ON articles (date);
                """)

                self.conn.commit()
                print("Table 'articles' and indexes created successfully.")

        except Exception as e:
            print(f"An error occurred while setting up the table: {e}")
            return False
        
    def save_news(self, news_data):
        if not self.conn:
            if not self.connect():
                return False
        
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO articles (id, title, link, image, date, topic, content_length)
                    VALUES (%(id)s, %(title)s, %(link)s, %(image)s, %(date)s, %(topic)s, %(content_length)s)
                    ON CONFLICT (link) DO NOTHING
                """, {
                    'id': news_data['id'],
                    'title': news_data['title'],
                    'link': news_data['link'],
                    'image': news_data['image'],
                    'date': news_data['date'],
                    'topic': news_data['topic'],
                    'content_length': len(news_data['content']) if news_data['content'] else 0
                })
                
                self.conn.commit()
                return True
                
        except Exception as e:
            print(f"Error saving article to PostgreSQL: {e}")
            return False
    
    def save_bulk(self, news_list):
        success_count = 0
        for news_data in news_list:
            if self.save_news(news_data):
                success_count += 1
        print(f"Successfully saved {success_count} news articles.")
        return success_count
    
    def get_stats(self):
        if not self.conn:
            if not self.connect():
                return None
        
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Total articles
                cur.execute("SELECT COUNT(*) as total FROM articles")
                total = cur.fetchone()['total']
                
                # Articles by topic
                cur.execute("""
                    SELECT topic, COUNT(*) as count 
                    FROM articles 
                    GROUP BY topic 
                    ORDER BY count DESC
                """)
                by_topic = cur.fetchall()
                
                # Recent articles
                cur.execute("""
                    SELECT COUNT(*) as recent 
                    FROM articles 
                    WHERE date >= NOW() - INTERVAL '3 days'
                """)
                recent = cur.fetchone()['recent']
                
                return {
                    'total_articles': total,
                    'by_topic': list(by_topic),
                    'recent_articles': recent
                }
                
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {}
    
    def close(self):
        if self.conn:
            self.conn.close()
            print("PostgreSQL connection closed.")
        else:
            print("No active PostgreSQL connection to close.")
