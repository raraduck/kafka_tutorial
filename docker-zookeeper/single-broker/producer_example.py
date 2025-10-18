#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaProducer
import json
import time
import random

def json_serializer(data):
    """데이터를 JSON 형식으로 직렬화하는 함수"""
    return json.dumps(data).encode('utf-8')

def get_random_message():
    """임의의 메시지를 생성하는 함수"""
    messages = [
        {"user_id": random.randint(1, 1000), "action": "login", "timestamp": time.time()},
        {"user_id": random.randint(1, 1000), "action": "purchase", "amount": random.randint(10, 1000), "timestamp": time.time()},
        {"user_id": random.randint(1, 1000), "action": "logout", "timestamp": time.time()},
        {"user_id": random.randint(1, 1000), "action": "view_page", "page": f"page_{random.randint(1, 100)}", "timestamp": time.time()}
    ]
    return random.choice(messages)

def on_send_success(record_metadata):
    """메시지 전송 성공 시 호출되는 콜백 함수"""
    print(f"메시지 전송 성공 - 토픽: {record_metadata.topic}, 파티션: {record_metadata.partition}, 오프셋: {record_metadata.offset}")

def on_send_error(excp):
    """메시지 전송 실패 시 호출되는 콜백 함수"""
    print(f"메시지 전송 실패: {excp}")

def main():
    # Kafka Producer 생성
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',  # Kafka 브로커 주소
        value_serializer=json_serializer,    # 값 직렬화 함수
        acks='all'                           # 메시지 수신 확인 레벨 (all: 모든 복제본 확인)
    )

    print("Kafka Producer 시작. Ctrl+C로 종료.")

    try:
        # 5초마다 메시지 전송
        while True:
            message = get_random_message()
            
            # 메시지 전송 (비동기)
            future = producer.send('user_activity', value=message)
            
            # 콜백 등록
            future.add_callback(on_send_success).add_errback(on_send_error)
            
            print(f"메시지 전송: {message}")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("프로듀서 종료")
    finally:
        # 남은 메시지 전송 및 리소스 해제
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main() 