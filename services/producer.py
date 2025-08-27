import os, json
from confluent_kafka import Producer
from dotenv import load_dotenv
load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "topic_raw")

class ProducerService:
    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP, topic=TOPIC_RAW):
        self.conf = {"bootstrap.servers": bootstrap_servers}
        self.topic = topic
        self.producer = Producer(self.conf)

    def _delivery_report(self, err, msg):
        if err:
            print(f"Delivery failed: {err}")

    def send_message(self, message: dict, key: str = None):
        try:
            self.producer.produce(self.topic, key=key.encode('utf-8') if key else None, value=json.dumps(message), callback=self._delivery_report)
            self.producer.flush()
            return True
        except Exception as e:
            print(f"Producer send error: {e}")
            return False

    def send_batch(self, messages: list):
        sent = 0
        try:
            for m in messages:
                self.producer.produce(self.topic, value=json.dumps(m).encode('utf-8'), callback=self._delivery_report)
                sent += 1
            self.producer.flush()
            return sent
        except Exception as e:
            print(f"Producer batch error: {e}")
            return sent

    def close(self):
        try:
            self.producer.flush()
        except:
            pass
