from kafka import KafkaConsumer
import json

def safe_deserializer(m):
    try:
        return json.loads(m.decode('utf-8'))
    except Exception:
        return m.decode('utf-8') if m else None

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=safe_deserializer
)

print("Waiting for messages...")
for message in consumer:
    print(f"Received message: {message.value}")
