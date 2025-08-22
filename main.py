import asyncio
from pipeline.cnn_news_kafka import ETLProcessKafka

async def main():
    """Main function to run the complete ETL pipeline with Kafka"""
    pipeline = ETLProcessKafka()
    
    print("Starting ETL Pipeline with Kafka...")
    print("=====================================")
    
    try:
        # Step 1: Setup all services
        print("\nStep 1: Setting up services...")
        pipeline.setup_services()
        
        # Step 2: Run the complete ETL process
        print("\nStep 2: Running ETL process...")
        result = await pipeline.run_complete_etl(total_page=5)
        
        if result:
            print("\nETL process completed successfully!")
            print("====================================")
            
            # Display results
            print(f"\nPipeline Results:")
            print(f"- Articles crawled: {result.get('crawled', 0)}")
            print(f"- Sent to Kafka: {result.get('kafka_sent', 0)}")
            print(f"- Bronze records: {result.get('bronze_saved', 0)}")
            print(f"- Silver records: {result.get('silver_processed', 0)}")
            print(f"- Gold records: {result.get('gold_processed', 0)}")
            print(f"- Elasticsearch saved: {result.get('es_saved', 0)}")
            print(f"- PostgreSQL saved: {result.get('pg_saved', 0)}")
            
            # Get statistics
            print(f"\nDatabase Statistics:")
            stats = pipeline.get_statistics()
            if stats:
                print(f"- Total articles in system: {stats.get('total_articles', 0)}")
                print(f"- Recent articles (3 days): {stats.get('recent_articles', 0)}")
                
                print(f"\nTop Topics:")
                for i, topic in enumerate(stats.get('by_topic', [])[:5], 1):
                    print(f"{i}. {topic['topic']}: {topic['count']} articles")
        else:
            print("\nETL process failed!")
            
    except Exception as e:
        print(f"\nError in main pipeline: {e}")
        raise
    finally:
        # Cleanup
        pipeline.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
    print("\n🎯 ETL Pipeline execution completed!")