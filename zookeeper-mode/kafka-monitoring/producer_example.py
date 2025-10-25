#!/usr/bin/env python3
"""
Kafka Producer 예제 - 고성능 메시지 생성
"""
import json
import random
import time
import argparse
from datetime import datetime
from confluent_kafka import Producer

def delivery_report(err, msg):
    """메시지 전송 결과 콜백"""
    if err is not None:
        print(f"메시지 전송 실패: {err}")
    else:
        print(f"메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset()}")

def main():
    # 인자 파싱
    parser = argparse.ArgumentParser(description='Kafka 프로듀서 예제 - 메시지 생성')
    parser.add_argument('--topic', default='test-lag', type=str, help='메시지를 보낼 토픽')
    parser.add_argument('--messages', default=100000, type=int, help='보낼 메시지 수')
    parser.add_argument('--size', default=100, type=int, help='메시지 크기 (바이트)')
    parser.add_argument('--rate', default=1000, type=float, help='초당 메시지 생성 속도')
    parser.add_argument('--bootstrap-servers', default='localhost:9092,localhost:9093,localhost:9094',
                     type=str, help='Kafka 부트스트랩 서버')
    args = parser.parse_args()

    # 프로듀서 설정
    producer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'performance-producer'
    }
    
    # Kafka 프로듀서 인스턴스 생성
    producer = Producer(producer_config)
    
    # 메시지 페이로드 템플릿
    message_template = {
        'timestamp': '',
        'user_id': 0,
        'message_id': 0,
        'data': ''
    }
    
    # 생성할 데이터 크기 계산 (헤더 크기 제외)
    data_size = max(0, args.size - 100)  # 헤더 크기 대략 100 바이트 가정
    
    print(f"토픽 {args.topic}에 메시지 {args.messages}개를 초당 {args.rate}개씩 전송합니다.")
    
    # 처리량 제한 설정
    interval = 1.0 / args.rate if args.rate > 0 else 0
    next_send_time = time.time()
    
    # 메시지 생성 및 전송
    for i in range(args.messages):
        # 메시지 생성
        message = message_template.copy()
        message['timestamp'] = datetime.now().isoformat()
        message['user_id'] = random.randint(1, 100)  # 임의의 사용자 ID
        message['message_id'] = i
        message['data'] = 'X' * data_size  # 원하는 크기만큼 데이터 채우기
        
        # JSON 형식으로 변환
        payload = json.dumps(message)
        
        # 처리량 제한 적용
        while time.time() < next_send_time:
            time.sleep(0.001)  # 짧은 대기
        
        # 메시지 전송
        producer.produce(
            args.topic,
            value=payload,
            key=str(message['user_id']),  # 사용자 ID를 키로 사용
            callback=delivery_report
        )
        
        # 다음 전송 시간 계산
        next_send_time = max(next_send_time + interval, time.time())
        
        # 간헐적으로 출력
        if i % 1000 == 0:
            print(f"{i}개 메시지 전송 완료")
            # producer.flush() 호출은 성능에 영향을 줄 수 있으므로 주의
        
        # 일정 간격으로 이벤트 처리 (배치 처리)
        if i % 100 == 0:
            producer.poll(0)
    
    # 남은 메시지 전송 완료 대기
    producer.flush()
    print(f"총 {args.messages}개 메시지 전송 완료!")

if __name__ == "__main__":
    main() 