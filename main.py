import asyncio
from pipeline.cnn_news import ETLProcess

async def main():
    pipeline = ETLProcess()
    pipeline.setup_services()
    result = await pipeline.run_etl(total_page=10)

    if result:
        print(f"ETL process completed successfully")

        stats = pipeline.get_statistic()
        if stats:
            print(f"DB Stats")
            print(f"Total articles in DB: {stats['total_articles']}")
            print(f"Recent articles (last 3 days): {stats['recent_articles']}")

            print(f"Articles by topic:")
            for topic in stats['by_topic']:
                print(f"Topic: {topic['topic']}, Count: {topic['count']}")
    
    else:
        print("ETL process failed.")

if __name__ == "__main__":
    asyncio.run(main())
    print("ETL process finished.")