from kafka import KafkaProducer
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class ProducerService:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BROKER')
        self.topic = os.getenv('KAFKA_TOPIC')

        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: json.dumps(k).encode('utf-8') if k is not None else None,
            acks='all',
            retries=5,
            linger_ms=10,
            batch_size=16384
        )

    def send_message(self, message, key=None):
        """Send a single message to Kafka"""
        try:
            # Convert message to ensure JSON serialization
            if not isinstance(message, dict):
                message = {"data": str(message)}
            
            future = self.producer.send(
                self.topic, 
                message, 
                key=key
            )
            
            # Get result with timeout
            record_metadata = future.get(timeout=30)
            
            print(f" Message sent to topic '{record_metadata.topic}' "
                  f"partition {record_metadata.partition} at offset {record_metadata.offset}")
            return True
            
        except Exception as e:
            print(f" Failed to send message: {e}")
            print(f"   Topic: {self.topic}")
            print(f"   Bootstrap servers: {self.bootstrap_servers}")
            return False
    
    def send_batch(self, messages):
        succes_count = 0
        for message in messages:
            key = message.get('id', None)
            if self.send_message(message, key=key):  # Perbaikan argumen
                succes_count += 1
        self.producer.flush()
        print(f"Batch send completed. {succes_count}/{len(messages)} messages sent successfully.")
        return succes_count
    
    def close(self):
        if self.producer:
            self.producer.close()
            print("Producer closed.")
        else:
            print("Producer was not initialized.")