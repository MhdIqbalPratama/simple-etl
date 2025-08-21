import asyncio
from crawler.cnn import CNNCrawler
from services.es_services import ESService
from services.pg_service import PGService
from processor.cleaner import Cleaner
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

class ETLProcess:
    def __init__(self, es_cloud=os.getenv("ES_CLOUD_URL"), es_api_key=os.getenv("ES_API_KEY"),
                 pg_host=os.getenv("PG_HOST"), pg_database=os.getenv("PG_DATABASE"),
                 pg_user=os.getenv("PG_USER"), pg_password=os.getenv("PG_PASSWORD"),
                 pg_port=os.getenv("PG_PORT")):
        self.crawler = CNNCrawler()
        self.cleaner = Cleaner()
        self.es_service = ESService()  # Fixed attribute name
        self.pg_service = PGService(pg_host, pg_database, pg_user, pg_password, pg_port)
    
    def setup_services(self):
        self.es_service.setup_index()
        self.pg_service.setup_table()
        print("Services setup completed.")

    async def run_etl(self, total_page=10):
        print("Starting ETL process...")
        try:
            print("Step 1 - Crawling news data...")
            raw_articles = await self.crawler.crawl_all_contents(total_page=total_page)
            print(f"Total articles crawled: {len(raw_articles)}")

            if not raw_articles:
                print("No articles found. Exiting ETL process.")
                return
            
            print("Step 2 - Cleaning data...")
            clean_articles = self.cleaner.clean_all_articles(raw_articles)
            print(f"Total articles after cleaning: {len(clean_articles)}")

            print("Step 3 - Saving to storage")

            es_success = self.es_service.save_bulk(clean_articles)
            pg_success = self.pg_service.save_bulk(clean_articles)

            # Show results
            print(f"\n ==== Pipelines Results ==== ")
            print(f"Total articles crawled: {len(raw_articles)}")
            print(f"Total articles after cleaning: {len(clean_articles)}")
            print(f"Total articles saved to Elasticsearch: {es_success}")
            print(f"Total articles saved to PostgreSQL: {pg_success}")

            return {
                "crawled": len(raw_articles),
                "cleaned": len(clean_articles),
                "es_saved": es_success,
                "pg_saved": pg_success
            }
        
        except Exception as e:
            print(f"An error occurred during the ETL process: {e}")
            return None
    
    def get_statistic(self):
        return self.pg_service.get_stats()