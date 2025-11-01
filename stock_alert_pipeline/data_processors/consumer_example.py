#!/usr/bin/env python3
"""
Kafka Consumer 예제 - 의도적으로 느린 컨슈머
"""
import json
import time
import argparse
import random
from confluent_kafka import Consumer, KafkaError, KafkaException

def main():
    # 인자 파싱
    parser = argparse.ArgumentParser(description='Kafka 컨슈머 예제 - 의도적으로 느린 처리')
    parser.add_argument('--topic', default='test-lag', type=str, help='구독할 토픽')
    parser.add_argument('--group', default='slow-consumer', type=str, help='컨슈머 그룹 ID')
    parser.add_argument('--min-bytes', default=10000, type=int, 
                     help='한 번에 가져올 최소 바이트 (기본값: 10000)')
    parser.add_argument('--max-records', default=100, type=int,
                     help='한 번에 가져올 최대 레코드 수 (기본값: 100)')
    parser.add_argument('--process-time', default=0.5, type=float,
                     help='레코드당 처리 시간(초) (기본값: 0.5)')
    parser.add_argument('--bootstrap-servers', default='localhost:9092,localhost:9093,localhost:9094',
                     type=str, help='Kafka 부트스트랩 서버')
    
    args = parser.parse_args()
    
    # 컨슈머 설정
    consumer_config = {
        'bootstrap.servers': args.bootstrap_servers,
        'group.id': args.group,
        'auto.offset.reset': 'earliest',  # 처음부터 읽기
        'enable.auto.commit': True,
        'auto.commit.interval.ms': 5000,  # 5초마다 오프셋 커밋
        'fetch.min.bytes': args.min_bytes,  # 최소 가져올 데이터 크기
        'fetch.wait.max.ms': 500,  # 데이터가 충분히 모일 때까지 최대 대기 시간
    }
    
    # 컨슈머 인스턴스 생성
    consumer = Consumer(consumer_config)
    
    # 토픽 구독
    consumer.subscribe([args.topic])
    
    # 토픽 메시지 소비
    try:
        print(f"토픽 {args.topic}에서 메시지를 소비합니다. 그룹 ID: {args.group}")
        print(f"각 메시지당 약 {args.process_time}초 동안 처리합니다.")
        
        message_count = 0
        start_time = time.time()
        
        while True:
            # 메시지 가져오기
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                # 메시지가 없으면 계속
                print("메시지가 없으면 계속")
                continue
            
            if msg.error():
                # 에러 발생 시 처리
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # 파티션 끝에 도달했을 때
                    print(f"파티션 {msg.partition()} 끝에 도달했습니다. 오프셋: {msg.offset()}")
                    continue
                else:
                    # 기타 에러
                    raise KafkaException(msg.error())
            
            # 메시지 처리
            message_count += 1
            
            try:
                # JSON 파싱
                value = json.loads(msg.value().decode('utf-8'))
                print(value)
                
                # 일정 비율로 상세 로그 출력
                if message_count % 100 == 0:
                    print(f"메시지 {message_count}개 처리됨, 파티션: {msg.partition()}, "
                          f"오프셋: {msg.offset()}, 키: {msg.key().decode('utf-8') if msg.key() else 'None'}")
                    
                    # 처리 속도 계산 및 출력
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        rate = message_count / elapsed
                        print(f"현재 처리 속도: {rate:.2f} 메시지/초")
                
                # 의도적으로 처리 시간 지연 (느린 컨슈머 시뮬레이션)
                # 변동성 추가: 기본 처리 시간의 80%~120% 범위에서 랜덤하게 지연
                process_time = args.process_time * random.uniform(0.8, 1.2)
                time.sleep(process_time)
                
            except json.JSONDecodeError:
                print(f"JSON 파싱 오류: {msg.value()}")
            except Exception as e:
                print(f"메시지 처리 오류: {e}")
    
    except KeyboardInterrupt:
        print("컨슈머 종료 요청됨...")
    finally:
        # 컨슈머 종료
        print(f"컨슈머 종료, 총 {message_count}개 메시지 처리됨")
        consumer.close()

if __name__ == "__main__":
    main() 