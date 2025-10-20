from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 메시지를 브로커로 보내기만 하고, 성공/실패 확인 로직은 전혀 추가하지 않음
producer.send('my_topic', value={'example': 'fire-and-forget'})

# Producer가 보낸 메시지를 내부 버퍼에 가지고 있을 수 있으므로,
# 종료 전 flush()나 close()를 해주는 것이 안전
producer.flush()  
producer.close()

print("Fire-and-forget 방식으로 메시지를 보냈습니다.") 