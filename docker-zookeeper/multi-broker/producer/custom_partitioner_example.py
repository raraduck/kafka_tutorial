# custom_partitioner_example.py
from kafka import KafkaProducer
from kafka.partitioner.default import DefaultPartitioner
import json
import random
import time

# 커스텀 파티셔너 클래스 정의
class CustomPartitioner(DefaultPartitioner):
    def __init__(self, num_partitions):
        self.num_partitions = num_partitions
    
    def partition(self, key, all_partitions, available_partitions):
        # 키가 없으면 기본 파티셔너 사용
        if key is None:
            return super().partition(key, all_partitions, available_partitions)
        
        # 키를 디코딩
        key_str = key.decode('utf-8')
        
        # 지역 코드를 기반으로 파티션 결정 (예시)
        if key_str.startswith('KR'):  # 한국
            return 0  # 항상 파티션 0으로
        elif key_str.startswith('US'):  # 미국
            return 1  # 항상 파티션 1로
        elif key_str.startswith('JP'):  # 일본
            return 2  # 항상 파티션 2로
        else:
            # 그 외 지역은 해시 기반으로 결정
            return hash(key_str) % len(available_partitions)

# 토픽의 파티션 수
NUM_PARTITIONS = 4

# 커스텀 파티셔너를 사용하는 Producer 생성
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    partitioner=CustomPartitioner(NUM_PARTITIONS)  # 커스텀 파티셔너 설정
)

# 테스트용 지역 코드
REGIONS = ['KR', 'US', 'JP', 'CN', 'UK', 'DE']

# 메시지 생성 및 전송
for _ in range(20):
    region = random.choice(REGIONS)
    user_id = f"{region}-{random.randint(1000, 9999)}"
    data = {
        "user_id": user_id,
        "region": region,
        "event": "login",
        "timestamp": time.time()
    }
    
    future = producer.send('my_topic', key=user_id, value=data)
    metadata = future.get(timeout=10)
    
    print(f"지역: {region}, 사용자: {user_id}, 파티션: {metadata.partition}")
    time.sleep(0.5)

producer.flush()
producer.close()