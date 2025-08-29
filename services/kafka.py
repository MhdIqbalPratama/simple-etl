from confluent_kafka import Consumer, KafkaException, KafkaError
import logging
import json
import os
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class KafkaService:
    def __init__(self, brokers: str, group_id: str):
        def __init__(self):
            self.bootstrap_servers = os.getenv('KAFKA_BROKER', 'localhost:9092')
            self.topic_raw = os.getenv('KAFKA_RAW_TOPIC')
            self.topic_clean = os.getenv('KAFKA_CLEAN_TOPIC')
        # Producer configuration
        self.producer_config = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all',
            'retries': 5,
            'retry.backoff.ms': 300,
            'linger.ms': 10,
            'batch.size': 32768,
            'compression.type': 'snappy',
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 1,
        }
        
        # Consumer configuration base
        self.consumer_config_base = {
            'bootstrap.servers': self.bootstrap_servers,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000,
            'session.timeout.ms': 30000,
            'heartbeat.interval.ms': 10000,
            'max.poll.interval.ms': 300000,
        }
        
        self.producer = None
        self.admin_client = None

    def subscribe(self, topics: list[str]):
        self.consumer.subscribe(topics)
        logger.info(f"Consumer subscribed to topics: {topics}")

    def consume_messages(self, batch_size: int = 100, timeout: int = 5):
        """
        Consume messages in batch mode.
        """
        messages = []
        try:
            while True:
                msg = self.consumer.poll(timeout)
                if msg is None:
                    break  # timeout selesai → keluar
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.warning(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                        continue
                    else:
                        raise KafkaException(msg.error())
                else:
                    try:
                        messages.append(json.loads(msg.value().decode("utf-8")))
                    except Exception as e:
                        logger.error(f"Failed to decode message: {e}")
                
                # Kalau sudah batch_size → stop
                if len(messages) >= batch_size:
                    break
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        return messages

    def commit(self):
        """
        Commit offset secara manual.
        """
        try:
            self.consumer.commit(asynchronous=False)
            logger.info("Offsets committed")
        except Exception as e:
            logger.error(f"Error committing offsets: {e}")

    def close(self):
        self.consumer.close()
        logger.info("Kafka consumer closed")
