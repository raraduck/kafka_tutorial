#!/usr/bin/env python3
"""
confluent-kafka-python 정확히 한 번 처리(Exactly-Once Processing) 테스트
"""

import json
import time
import uuid
import sys
import signal
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException

# 종료 플래그
running = True

def signal_handler(sig, frame):
    """시그널 핸들러 - Ctrl+C 처리"""
    global running
    print("\n종료 신호를 받았습니다. 안전하게 종료합니다...")
    running = False

def create_test_messages(bootstrap_servers='localhost:9092,localhost:9093,localhost:9094', topic='input-topic', num_messages=10):
    """테스트용 메시지 생성"""
    
    # 일반 프로듀서 설정
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': f'test-data-producer-{uuid.uuid4()}'
    }
    
    print(f"\n[테스트 데이터 생성]")
    print(f"총 {num_messages}개의 메시지를 {topic} 토픽에 생성합니다.")
    
    # 프로듀서 생성
    producer = Producer(producer_config)
    
    try:
        for i in range(num_messages):
            # 테스트 메시지 데이터
            message = {
                'id': i,
                'text': f'테스트 메시지 #{i}',
                'value': i * 10,
                'timestamp': time.time()
            }
            
            # 메시지 전송
            producer.produce(
                topic=topic,
                key=str(i),
                value=json.dumps(message).encode('utf-8'),
                callback=lambda err, msg: print(f"메시지 전송 {'실패' if err else '성공'}: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset() if not err else 'N/A'}")
            )
            
            # 이벤트 처리
            producer.poll(0)
            print(f"테스트 메시지 #{i} 전송 요청됨")
            
            # 약간의 지연
            time.sleep(0.1)
        
        # 모든 메시지 전송 완료 대기
        print("\n[메시지 flush 중...]")
        remaining = producer.flush(timeout=10)
        
        if remaining > 0:
            print(f"⚠️ {remaining}개의 메시지가 전송되지 않았습니다.")
        else:
            print(f"✅ 모든 테스트 메시지가 성공적으로 생성되었습니다.")
        
        return num_messages - remaining
        
    except Exception as e:
        print(f"❌ 테스트 데이터 생성 중 오류 발생: {e}")
        return 0


def create_transaction_consumer_producer(bootstrap_servers='localhost:9092,localhost:9093,localhost:9094', group_id=None):
    """트랜잭션 컨슈머-프로듀서 생성"""
    
    # 고유한 그룹 ID 생성 (없으면)
    if group_id is None:
        group_id = f'txn-consumer-group-{int(time.time())}'
    
    # 트랜잭션 ID 생성 (고유해야 함)
    transaction_id = f'txn-consumer-producer-{uuid.uuid4()}'
    
    # 트랜잭션 프로듀서 설정
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'transactional.id': transaction_id,  # 트랜잭션 ID (필수)
        'enable.idempotence': True,          # 트랜잭션은 자동으로 멱등성 활성화
        'acks': 'all'                        # 필수 설정
    }
    
    # 컨슈머 설정
    consumer_config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'isolation.level': 'read_committed',  # 커밋된 트랜잭션만 읽기
        'enable.auto.commit': False,          # 수동 커밋 모드 필수
        'auto.offset.reset': 'earliest'
    }
    
    print("\n[트랜잭션 프로듀서 설정]")
    for key, value in producer_config.items():
        print(f"  {key}: {value}")
    
    print("\n[컨슈머 설정]")
    for key, value in consumer_config.items():
        print(f"  {key}: {value}")
    
    # 프로듀서 생성 및 초기화
    producer = Producer(producer_config)
    print("\n[트랜잭션 초기화 중...]")
    producer.init_transactions()
    print("✅ 트랜잭션 초기화 성공")
    
    # 컨슈머 생성
    consumer = Consumer(consumer_config)
    
    return producer, consumer, group_id


def process_data(value):
    """데이터 처리 함수 - 실제 비즈니스 로직"""
    # 여기서는 간단히 JSON 데이터에 'processed' 필드를 추가
    try:
        data = json.loads(value.decode('utf-8'))
        data['processed'] = True
        data['processed_timestamp'] = time.time()
        return json.dumps(data).encode('utf-8')
    except:
        return value  # 처리 실패 시 원본 반환


def process_messages(input_topic, output_topic, producer, consumer, batch_size=10, max_batches=5):
    """메시지 처리 주 함수"""
    global running
    
    # Ctrl+C 시그널 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 토픽 구독
    consumer.subscribe([input_topic])
    print(f"✅ 토픽 구독: {input_topic}")
    
    # 통계 변수
    total_processed = 0
    batch_count = 0
    
    try:
        while running and batch_count < max_batches:
            # 메시지 배치 읽기
            messages = consumer.consume(timeout=5.0, num_messages=batch_size)
            if not messages:
                print("더 이상 메시지가 없습니다. 대기 중...")
                time.sleep(1)
                continue
                
            print(f"{len(messages)}개의 메시지를 읽었습니다.")
            
            # 트랜잭션 시작
            producer.begin_transaction()
            
            offsets = []  # 커밋할 오프셋 저장
            
            # 메시지 처리 및 변환
            for msg in messages:
                if msg.error():
                    print(f"오류: {msg.error()}")
                    continue
                    
                # 오프셋 정보 저장
                offsets.append(TopicPartition(
                    msg.topic(), 
                    msg.partition(), 
                    msg.offset() + 1
                ))
                
                # 메시지 처리 로직
                processed_value = process_data(msg.value())
                
                # 결과 메시지 전송
                producer.produce(
                    output_topic,
                    key=msg.key(),
                    value=processed_value
                )
                producer.poll(0)
            
            if offsets:
                try:
                    print("처리된 메시지를 출력 토픽에 쓰고 입력 오프셋 커밋 중...")
                    
                    # 컨슈머 오프셋도 트랜잭션의 일부로 커밋
                    producer.send_offsets_to_transaction(
                        offsets, 
                        consumer.consumer_group_metadata()
                    )
                    
                    # 트랜잭션 커밋 (메시지 전송 + 오프셋 커밋)
                    producer.commit_transaction()
                    print(f"트랜잭션 커밋 완료: 배치 #{batch_count+1}")
                    batch_count += 1
                    total_processed += len(messages)
                    
                except KafkaException as e:
                    print(f"트랜잭션 오류: {e}")
                    producer.abort_transaction()
            else:
                producer.abort_transaction()
    
    except KeyboardInterrupt:
        print("\n키보드 인터럽트 감지, 안전하게 종료합니다...")
    
    finally:
        # 통계 출력
        print("\n처리 통계:")
        print(f"  총 처리 메시지: {total_processed}")
        print(f"  총 배치: {batch_count}")
        
        # 컨슈머 종료
        consumer.close()
        print("컨슈머가 닫혔습니다.")
    
    return total_processed


if __name__ == "__main__":
    input_topic = 'input-topic'
    output_topic = 'output-topic'
    
    print(f"입력 토픽: {input_topic}, 출력 토픽: {output_topic}")
    
    if len(sys.argv) > 1 and sys.argv[1] == 'generate':
        # 테스트 메시지 생성 모드
        num_messages = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        created = create_test_messages(topic=input_topic, num_messages=num_messages)
        print(f"\n테스트 메시지 생성 완료: {created}/{num_messages}")
    else:
        # 메시지 처리 모드
        # 트랜잭션 컨슈머-프로듀서 생성
        producer, consumer, group_id = create_transaction_consumer_producer()
        
        # 메시지 처리
        batch_size = int(sys.argv[1]) if len(sys.argv) > 1 else 5
        max_batches = int(sys.argv[2]) if len(sys.argv) > 2 else 10
        
        print(f"배치 크기: {batch_size}, 최대 배치 수: {max_batches}")
        processed = process_messages(
            input_topic, 
            output_topic,
            producer, 
            consumer,
            batch_size,
            max_batches
        )
        
        print(f"\n정확히 한 번 처리 테스트 완료: {processed}개 메시지 처리됨") 