from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message_data = {"user_id": 123, "message": "Hello Kafka!"}
producer.send('my_topic', value=message_data)
producer.flush()

print("Message sent successfully!") 