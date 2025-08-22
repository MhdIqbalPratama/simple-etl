from kafka import KafkaProducer
import json
p = KafkaProducer(bootstrap_servers='localhost:9092',
                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))
p.send('test-topic', {'msg':'halo dari python'})
p.flush()
print("sent")
