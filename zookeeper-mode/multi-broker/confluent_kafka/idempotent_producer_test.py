#!/usr/bin/env python3
"""
confluent-kafka-python 멱등성 프로듀서 테스트
"""

import json
import time
import uuid
import sys
from confluent_kafka import Producer, KafkaException

def delivery_callback(err, msg):
    """메시지 전송 콜백"""
    if err is not None:
        print(f'메시지 전송 실패: {err}')
    else:
        print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset()}')


def create_idempotent_producer(bootstrap_servers='localhost:9092,localhost:9093,localhost:9094'):
    """멱등성 프로듀서 생성"""
    # 멱등성 프로듀서 설정
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': f'idempotent-producer-{uuid.uuid4()}',
        'enable.idempotence': True,  # 멱등성 활성화 (핵심 설정)
        'acks': 'all',               # 자동으로 'all'로 설정됨
        'retries': 5,                # 재시도 횟수
        'max.in.flight.requests.per.connection': 5  # 최대 5로 제한됨
    }
    
    print("\n[프로듀서 설정]")
    for key, value in producer_config.items():
        print(f"  {key}: {value}")
    
    # 프로듀서 생성
    producer = Producer(producer_config)
    return producer


def send_messages(producer, topic, num_messages=5):
    """메시지 전송"""
    successful_messages = 0
    
    try:
        print("\n[메시지 전송 시작]")
        for i in range(num_messages):
            # 메시지 데이터
            message = {
                'id': i,
                'message': f'멱등성 프로듀서 테스트 메시지 #{i}',
                'timestamp': time.time()
            }
            
            # 메시지 전송
            producer.produce(
                topic=topic,
                key=str(i),
                value=json.dumps(message).encode('utf-8'),
                callback=delivery_callback
            )
            
            # 이벤트 처리
            producer.poll(0)
            
            print(f"  메시지 #{i} 전송 요청됨")
            time.sleep(0.5)  # 메시지간 간격
            successful_messages += 1
        
        # 모든 메시지가 전송될 때까지 대기
        print("\n[남은 메시지 flush 중...]")
        remaining = producer.flush(timeout=10)
        
        if remaining > 0:
            print(f"❗ {remaining}개의 메시지가 전송되지 않았습니다.")
        else:
            print("✅ 모든 메시지 flush 완료")
        
    except KafkaException as e:
        print(f"\n❌ Kafka 예외 발생: {e}")
        return successful_messages
    except Exception as e:
        print(f"\n❌ 일반 예외 발생: {e}")
        return successful_messages
    
    return successful_messages


if __name__ == "__main__":
    if len(sys.argv) > 1:
        topic = sys.argv[1]
    else:
        topic = 'idempotent-topic'
        
    if len(sys.argv) > 2:
        num_messages = int(sys.argv[2])
    else:
        num_messages = 10
    
    print(f"토픽: {topic}, 메시지 수: {num_messages}")
    
    # 프로듀서 생성
    producer = create_idempotent_producer()
    
    # 메시지 전송
    successful_count = send_messages(producer, topic, num_messages)
    
    print(f"\n테스트 결과: {successful_count}/{num_messages} 메시지 전송 성공")
    
    if successful_count == num_messages:
        print("\n✅ 멱등성 프로듀서 테스트 완료")
    else:
        print("\n❌ 멱등성 프로듀서 테스트 부분 실패") 