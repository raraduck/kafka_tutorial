#!/usr/bin/env python3
"""
confluent-kafka-python 브로커 장애 테스트
"""

import json
import time
import uuid
import sys
import threading
import signal
import subprocess
from confluent_kafka import Producer, KafkaException

# 종료 플래그
running = True

def signal_handler(sig, frame):
    """시그널 핸들러 - Ctrl+C 처리"""
    global running
    print("\n종료 신호를 받았습니다. 안전하게 종료합니다...")
    running = False

def delivery_callback(err, msg):
    """메시지 전송 콜백"""
    if err is not None:
        print(f'메시지 전송 실패: {err}')
    else:
        print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset()}')

def create_producer(bootstrap_servers='localhost:9092,localhost:9093,localhost:9094', idempotent=True):
    """프로듀서 생성"""
    # 프로듀서 설정
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': f'producer-{uuid.uuid4()}',
    }
    
    if idempotent:
        producer_config.update({
            'enable.idempotence': True,  # 멱등성 활성화
            'acks': 'all',               # 자동으로 'all'로 설정됨
            'retries': 10,               # 재시도 횟수 증가
            'retry.backoff.ms': 250,     # 재시도 간격
            'max.in.flight.requests.per.connection': 5  # 최대 5로 제한됨
        })
    
    print("\n[프로듀서 설정]")
    for key, value in producer_config.items():
        print(f"  {key}: {value}")
    
    # 프로듀서 생성
    producer = Producer(producer_config)
    return producer


def stop_broker(broker="kafka2"):
    """브로커 중지"""
    print(f"\n[브로커 중지: {broker}]")
    try:
        subprocess.run(["docker", "stop", broker], check=True)
        print(f"✅ {broker} 브로커가 중지되었습니다.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {broker} 브로커 중지 실패: {e}")
        return False

def start_broker(broker="kafka2"):
    """브로커 시작"""
    print(f"\n[브로커 시작: {broker}]")
    try:
        subprocess.run(["docker", "start", broker], check=True)
        print(f"✅ {broker} 브로커가 시작되었습니다.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {broker} 브로커 시작 실패: {e}")
        return False

def send_messages_with_broker_failure(producer, topic, num_messages=20, failure_point=10, recovery_delay=10):
    """브로커 장애 시뮬레이션 후 메시지 전송"""
    global running
    
    # Ctrl+C 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    successful_messages = 0
    failed_messages = 0
    
    try:
        print(f"\n[브로커 장애 시뮬레이션 메시지 전송 시작] 총 {num_messages}개, 장애 지점: {failure_point}")
        
        for i in range(num_messages):
            if not running:
                break
                
            # 브로커 장애 시뮬레이션
            if i == failure_point:
                print(f"\n===== 브로커 장애 시뮬레이션 시작 (메시지 #{i}) =====")
                stop_broker("kafka2")
                
                # 브로커 장애 후 자동 복구 스레드 시작
                if recovery_delay > 0:
                    threading.Thread(
                        target=lambda: (time.sleep(recovery_delay), start_broker("kafka2"))
                    ).start()
            
            # 메시지 데이터
            message = {
                'id': i,
                'message': f'브로커 장애 테스트 메시지 #{i}',
                'timestamp': time.time()
            }
            
            try:
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
                successful_messages += 1
                
            except KafkaException as e:
                print(f"❌ 메시지 #{i} 전송 실패: {e}")
                failed_messages += 1
                
            except Exception as e:
                print(f"❌ 일반 오류 발생 (메시지 #{i}): {e}")
                failed_messages += 1
            
            # 메시지 간 간격
            time.sleep(0.5)
        
        # 모든 메시지가 전송될 때까지 대기
        print("\n[남은 메시지 flush 중...]")
        remaining = producer.flush(timeout=30)
        
        if remaining > 0:
            print(f"❗ {remaining}개의 메시지가 전송되지 않았습니다.")
            failed_messages += remaining
            successful_messages -= remaining
        else:
            print("✅ 모든 메시지 flush 완료")
        
    except KeyboardInterrupt:
        print("\n사용자에 의한 중단")
    except Exception as e:
        print(f"\n❌ 예외 발생: {e}")
    finally:
        # 브로커가 아직 중지 상태면 재시작
        start_broker("kafka2")
    
    return successful_messages, failed_messages

if __name__ == "__main__":
    # 토픽 설정
    topic = 'idempotent-topic'
    
    # 테스트 설정
    num_messages = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    failure_point = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    recovery_delay = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    idempotent = True if len(sys.argv) <= 4 or sys.argv[4].lower() != 'false' else False
    
    print(f"토픽: {topic}")
    print(f"메시지 수: {num_messages}, 장애 지점: {failure_point}, 복구 지연: {recovery_delay}초")
    print(f"멱등성 설정: {'활성화' if idempotent else '비활성화'}")
    
    # 프로듀서 생성
    producer = create_producer(idempotent=idempotent)
    
    # 브로커 장애 시뮬레이션 메시지 전송
    successful, failed = send_messages_with_broker_failure(
        producer, 
        topic, 
        num_messages,
        failure_point,
        recovery_delay
    )
    
    # 결과 출력
    print(f"\n===== 브로커 장애 테스트 결과 =====")
    print(f"성공한 메시지: {successful}/{num_messages}")
    print(f"실패한 메시지: {failed}/{num_messages}")
    
    if successful == num_messages:
        print("\n✅ 테스트 성공 - 모든 메시지가 전송되었습니다.")
    else:
        print(f"\n❗ 테스트 부분 성공 - {num_messages-successful}개 메시지 전송 실패") 