#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer
import json

def json_deserializer(data):
    """바이트를 JSON 객체로 역직렬화하는 함수"""
    return json.loads(data.decode('utf-8'))

def process_message(message):
    """메시지를 처리하는 함수"""
    action = message.get('action', 'unknown')
    
    if action == 'login':
        print(f"🟢 사용자 {message['user_id']}가 로그인했습니다.")
    elif action == 'logout':
        print(f"🔴 사용자 {message['user_id']}가 로그아웃했습니다.")
    elif action == 'purchase':
        print(f"💰 사용자 {message['user_id']}가 {message['amount']}원 결제했습니다.")
    elif action == 'view_page':
        print(f"👁️ 사용자 {message['user_id']}가 {message['page']} 페이지를 조회했습니다.")
    else:
        print(f"⚪ 알 수 없는 액션: {message}")

def main():
    # Kafka Consumer 생성
    consumer = KafkaConsumer(
        'user_activity',                       # 구독할 토픽
        bootstrap_servers='localhost:9092',    # Kafka 브로커 주소
        auto_offset_reset='earliest',          # 처음부터 메시지 읽기 (latest: 최신 메시지부터)
        enable_auto_commit=True,               # 자동 오프셋 커밋 활성화
        group_id='user_activity_group',        # 컨슈머 그룹 ID
        value_deserializer=json_deserializer,  # 값 역직렬화 함수
        consumer_timeout_ms=1000               # 1초 동안 메시지가 없으면 타임아웃 (선택 사항)
    )

    print("Kafka Consumer 시작. Ctrl+C로 종료.")
    print("'user_activity' 토픽 메시지 대기 중...")

    try:
        # 메시지 소비 루프
        for message in consumer:
            print(f"\n======= 새 메시지 수신 (파티션: {message.partition}, 오프셋: {message.offset}) =======")
            process_message(message.value)
            
    except KeyboardInterrupt:
        print("\n컨슈머 종료")
    finally:
        # 리소스 해제
        consumer.close()

if __name__ == "__main__":
    main() 