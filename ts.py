import logging
from services.kafka_services import KafkaService
from services.batch_pg import PostgresService
kafka_service = KafkaService()
pg_service = PostgresService()
import os
# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting bronze batch loader...")
    
# Setup services
if not kafka_service.setup_topics():
    raise Exception("Failed to setup Kafka topics")

if not pg_service.connect():
    raise Exception("Failed to connect to PostgreSQL")
    
if not pg_service.setup_database():
    raise Exception("Failed to setup database")

# Create consumer for raw topic
group_id = 'bronze_batch_loader'
consumer = kafka_service.get_consumer(
    group_id=group_id,
    topics=[kafka_service.topic_raw]
)

# Collect messages in batch
batch_articles = []
max_batch_size = 200
timeout_seconds = 5

# Consume messages for specified timeout
import time
start_time = time.time()

logger.info(f"Consuming from raw topic (max: {max_batch_size}, timeout: {timeout_seconds}s)")

while (time.time() - start_time) < timeout_seconds and len(batch_articles) < max_batch_size:
    processed_count = kafka_service.consume_messages(
        consumer=consumer,
        message_handler=lambda msg: batch_articles.append(msg),
        timeout=1.0,
        max_messages=50
    )
    
    if processed_count == 0:
        time.sleep(0.5)  # Short pause if no messages

consumer.close()
