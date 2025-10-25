from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 메시지 전송 후, 결과(성공/실패) 응답을 기다림
future = producer.send('my_topic', value={'example': 'synchronous'})

try:
    record_metadata = future.get(timeout=10)  # 10초 내에 응답 대기                         

    print(f"Topic: {record_metadata.topic}")
    print(f"Partition: {record_metadata.partition}")
    print(f"Offset: {record_metadata.offset}")
except KafkaError as e:
    print(f"메시지 전송 실패: {e}")

producer.flush()
producer.close()

print("동기 방식으로 메시지를 전송했습니다.") 