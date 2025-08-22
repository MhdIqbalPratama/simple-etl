from kafka import KafkaConsumer
import json
import os
from dotenv import load_dotenv
# Load environment variables from .env file
load_dotenv()

class ConsumerService:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BROKER')
        self.topic = os.getenv('KAFKA_TOPIC')

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=[self.bootstrap_servers],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: json.loads(k.decode('utf-8')) if k else None
        )

    def consume_messages(self, max_messages=None):
        """Consume messages from Kafka"""
        messages = []
        message_count = 0
        
        print(f"Starting to consume messages from topic '{self.topic}'...")
        
        try:
            for message in self.consumer:
                messages.append({
                    'key': message.key,
                    'value': message.value,
                    'partition': message.partition,
                    'offset': message.offset,
                    'timestamp': message.timestamp
                })
                
                message_count += 1
                print(f"Consumed message {message_count} from partition {message.partition} at offset {message.offset}")
                
                if max_messages and message_count >= max_messages:
                    break
                    
        except KeyboardInterrupt:
            print("Consumer interrupted by user")
        except Exception as e:
            print(f"Error consuming messages: {e}")
        
        print(f"Total messages consumed: {len(messages)}")
        return messages
    
    def consume_batch(self, timeout_ms=10000, max_messages=100):
        """Consume a batch of messages with timeout"""
        messages = []
        
        try:
            msg_pack = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_messages)
            
            for topic_partition, msgs in msg_pack.items():
                for message in msgs:
                    messages.append(message.value)
                    print(f"Consumed message from partition {message.partition} at offset {message.offset}")
            
        except Exception as e:
            print(f"Error consuming batch: {e}")
        
        print(f"Consumed batch of {len(messages)} messages")
        return messages
    
    def close(self):
        """Close the consumer"""
        self.consumer.close()
        print("Kafka consumer closed")