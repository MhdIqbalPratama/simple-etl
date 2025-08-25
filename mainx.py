import os
import sys
import signal
import asyncio
import argparse
from datetime import datetime

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Import the streaming consumer
from pipeline.consumer_pipeline import StreamingConsumer

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    print(f"\n🛑 Received signal {signum}. Shutting down gracefully...")
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(
        description='Streaming ETL Pipeline Consumer',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_streaming.py                    # Run with default settings
  python run_streaming.py --stats-interval 60  # Print stats every 60 seconds
  python run_streaming.py --help             # Show this help message

Environment Variables Required:
  KAFKA_BROKER    - Kafka broker address (e.g., localhost:9092)
  KAFKA_TOPIC     - Kafka topic to consume from
  PG_HOST         - PostgreSQL host
  PG_DATABASE     - PostgreSQL database name
  PG_USER         - PostgreSQL username
  PG_PASSWORD     - PostgreSQL password
  PG_PORT         - PostgreSQL port
        """
    )
    
    parser.add_argument(
        '--stats-interval', 
        type=int, 
        default=30,
        help='Statistics print interval in seconds (default: 30)'
    )
    
    parser.add_argument(
        '--max-batch-size',
        type=int,
        default=50,
        help='Maximum number of messages to process in one batch (default: 50)'
    )
    
    parser.add_argument(
        '--poll-timeout',
        type=int,
        default=5000,
        help='Kafka consumer poll timeout in milliseconds (default: 5000)'
    )
    
    parser.add_argument(
        '--check-env',
        action='store_true',
        help='Check environment variables and exit'
    )
    
    args = parser.parse_args()
    
    # Check environment variables
    required_env_vars = [
        'KAFKA_BROKER',
        'KAFKA_TOPIC', 
        'PG_HOST',
        'PG_DATABASE',
        'PG_USER',
        'PG_PASSWORD',
        'PG_PORT'
    ]
    
    missing_vars = []
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables in your .env file or environment.")
        sys.exit(1)
    
    if args.check_env:
        print("✅ All required environment variables are set:")
        for var in required_env_vars:
            value = os.getenv(var)
            # Hide password
            if 'PASSWORD' in var:
                display_value = '*' * len(value) if value else 'NOT SET'
            else:
                display_value = value
            print(f"   {var}: {display_value}")
        sys.exit(0)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("🚀 Starting Streaming ETL Pipeline")
    print("="*50)
    print(f"📅 Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Kafka Topic: {os.getenv('KAFKA_TOPIC')}")
    print(f"🏢 Kafka Broker: {os.getenv('KAFKA_BROKER')}")
    print(f"🗄️  Database: {os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DATABASE')}")
    print(f"📊 Stats Interval: {args.stats_interval} seconds")
    print(f"📦 Max Batch Size: {args.max_batch_size}")
    print("="*50)
    print("Press Ctrl+C to stop the pipeline")
    print()
    
    async def run_pipeline():
        consumer = StreamingConsumer()
        
        # Apply configuration from command line args
        # (You could extend StreamingConsumer to accept these parameters)
        
        try:
            await consumer.start_streaming()
        except KeyboardInterrupt:
            print("\n🛑 Pipeline interrupted by user")
        except Exception as e:
            print(f"❌ Pipeline failed with error: {e}")
            sys.exit(1)
        finally:
            consumer.stop()
            print("👋 Pipeline stopped. Goodbye!")
    
    # Run the streaming pipeline
    try:
        asyncio.run(run_pipeline())
    except KeyboardInterrupt:
        print("\n🛑 Received interrupt signal")
    except Exception as e:
        print(f"❌ Failed to start pipeline: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()