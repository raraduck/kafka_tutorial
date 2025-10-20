from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8')  # 키를 바이트로 변환
)

# 🔹 user_id를 제한된 범위에서 선택하여 같은 파티션에 묶이도록 함.
USER_IDS = [1001, 1002, 1003]  # 특정 몇 개의 user_id만 사용

def generate_message():
    user_id = random.choice(USER_IDS)  # 제한된 user_id 중 하나 선택
    return user_id, {
        "user_id": user_id,
        "event_type": random.choice(["click", "purchase", "view"]),
        "timestamp": time.time()
    }

# 여러 개의 메시지를 전송하면서 파티셔닝 테스트
for _ in range(20):
    key, message_data = generate_message()
    producer.send('my_topic', key=key, value=message_data)  # user_id를 key로 설정
    print(f"Sent: {message_data} to partition with key {key}")
    time.sleep(0.5)

producer.flush()
print("Partitioned messages sent successfully!") 