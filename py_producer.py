from kafka import KafkaProducer
import json
p = KafkaProducer(bootstrap_servers='localhost:9092',
                  value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# p.send('test-topic', {'msg':'halo dari python'})
# p.send('test-topic', { 'msg': 'This topic to prove streaming consume'})
# for i in range(5):
#     p.send('test-topic', {'msg': f"this is {i} messages"})
msg = input("send message : ")
p.send('test-topic', {'msg' : msg})
p.flush()
print("sent")
