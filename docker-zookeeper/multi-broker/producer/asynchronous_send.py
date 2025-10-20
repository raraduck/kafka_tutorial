from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
    print("비동기 전송 성공!")
    print(f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")

def on_send_error(excp):
    print(f"비동기 전송 실패: {excp}")

# 메시지 전송 후, 콜백 등록으로 성공/실패 정보를 비동기로 처리
future = producer.send('my_topic', value={'example': 'asynchronous'})
future.add_callback(on_send_success).add_errback(on_send_error)

# 메인 스레드는 즉시 다음 작업을 수행 가능
# 다만 종료 전에는 buffer에 남은 메시지를 flush() 해야 함
producer.flush()
producer.close()

print("비동기 방식으로 메시지를 전송했습니다(콜백 확인).") 