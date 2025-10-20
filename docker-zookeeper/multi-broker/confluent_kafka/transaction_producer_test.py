#!/usr/bin/env python3
"""
confluent-kafka-python 트랜잭션 프로듀서 테스트
"""

import json
import time
import uuid
import sys
import random
from confluent_kafka import Producer, KafkaException

def create_transaction_producer(bootstrap_servers='localhost:9092,localhost:9093,localhost:9094'):
    """트랜잭션 프로듀서 생성"""
    # 트랜잭션 ID 생성 (고유해야 함)
    transaction_id = f'txn-producer-{uuid.uuid4()}'
    
    # 트랜잭션 프로듀서 설정
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
        'transactional.id': transaction_id,  # 트랜잭션 ID (필수)
        'enable.idempotence': True,   # 트랜잭션은 자동으로 멱등성 활성화
        'acks': 'all',                # 필수 설정
        'transaction.timeout.ms': 60000  # 트랜잭션 타임아웃 60초
    }
    
    print("\n[트랜잭션 프로듀서 설정]")
    for key, value in producer_config.items():
        print(f"  {key}: {value}")
    
    # 프로듀서 생성
    producer = Producer(producer_config)
    
    # 트랜잭션 초기화
    print("\n[트랜잭션 초기화 중...]")
    producer.init_transactions()
    print("✅ 트랜잭션 초기화 성공")
    
    return producer


def run_successful_transaction(producer, order_topic, payment_topic):
    """성공하는 트랜잭션 예제"""
    print("\n----- 성공 트랜잭션 실행 -----")
    
    try:
        # 트랜잭션 시작
        producer.begin_transaction()
        print("✅ 트랜잭션 시작")
        
        # 주문 생성 이벤트 전송
        order_id = random.randint(1000, 9999)
        
        order_data = {
            'order_id': order_id,
            'customer_id': 'user123',
            'product_id': 'product456',
            'quantity': 2,
            'status': 'created',
            'timestamp': time.time()
        }
        
        producer.produce(
            topic=order_topic,
            key=f'order-{order_id}',
            value=json.dumps(order_data).encode('utf-8')
        )
        producer.poll(0)
        print(f"✅ 주문 이벤트 전송 요청됨: order_id={order_id}")
        
        # 결제 처리 이벤트 전송
        payment_data = {
            'order_id': order_id,
            'payment_method': 'credit_card',
            'amount': 129.99,
            'status': 'processing',
            'timestamp': time.time()
        }
        
        producer.produce(
            topic=payment_topic,
            key=f'order-{order_id}',
            value=json.dumps(payment_data).encode('utf-8')
        )
        producer.poll(0)
        print(f"✅ 결제 이벤트 전송 요청됨: order_id={order_id}")
        
        # 처리 지연 시뮬레이션
        time.sleep(1)
        
        # 트랜잭션 커밋
        print("[트랜잭션 커밋 중...]")
        producer.commit_transaction()
        print("✅ 트랜잭션이 성공적으로 커밋되었습니다.")
        
        return order_id, True
        
    except Exception as e:
        print(f"❌ 트랜잭션 실패: {e}")
        try:
            producer.abort_transaction()
            print("✅ 트랜잭션이 중단되었습니다.")
        except Exception as abort_e:
            print(f"❌ 트랜잭션 중단 실패: {abort_e}")
        return None, False


def run_aborted_transaction(producer, order_topic, payment_topic):
    """중단되는 트랜잭션 예제"""
    print("\n----- 중단 트랜잭션 실행 -----")
    
    try:
        # 트랜잭션 시작
        producer.begin_transaction()
        print("✅ 트랜잭션 시작")
        
        # 주문 생성 이벤트 전송
        order_id = random.randint(1000, 9999)
        
        order_data = {
            'order_id': order_id,
            'customer_id': 'user456',
            'product_id': 'product789',
            'quantity': 1,
            'status': 'created',
            'timestamp': time.time()
        }
        
        producer.produce(
            topic=order_topic,
            key=f'order-{order_id}',
            value=json.dumps(order_data).encode('utf-8')
        )
        producer.poll(0)
        print(f"✅ 주문 이벤트 전송 요청됨: order_id={order_id}")
        
        # 결제 처리 이벤트 전송 (실패 케이스)
        payment_data = {
            'order_id': order_id,
            'payment_method': 'credit_card',
            'amount': 99.99,
            'status': 'failed',  # 결제 실패
            'error_code': 'insufficient_funds',
            'timestamp': time.time()
        }
        
        producer.produce(
            topic=payment_topic,
            key=f'order-{order_id}',
            value=json.dumps(payment_data).encode('utf-8')
        )
        producer.poll(0)
        print(f"✅ 결제 이벤트 전송 요청됨: order_id={order_id}, status=failed")
        
        # 비즈니스 로직 검증 - 결제 실패 시 트랜잭션 중단
        print("[비즈니스 로직 검증 중... 결제 실패 감지]")
        time.sleep(1)
        
        # 실패 시 트랜잭션 중단
        print("[트랜잭션 중단 중...]")
        producer.abort_transaction()
        print("✅ 트랜잭션이 의도적으로 중단되었습니다.")
        
        return order_id, True
        
    except Exception as e:
        print(f"❌ 트랜잭션 처리 중 오류 발생: {e}")
        try:
            producer.abort_transaction()
            print("✅ 트랜잭션이 중단되었습니다.")
        except Exception as abort_e:
            print(f"❌ 트랜잭션 중단 실패: {abort_e}")
        return None, False


if __name__ == "__main__":
    order_topic = 'order-events'
    payment_topic = 'payment-events'
    
    print(f"주문 토픽: {order_topic}, 결제 토픽: {payment_topic}")
    
    # 트랜잭션 프로듀서 생성
    producer = create_transaction_producer()
    
    # 성공 트랜잭션 실행
    success_order_id, success_result = run_successful_transaction(producer, order_topic, payment_topic)
    
    # 1초 대기
    time.sleep(1)
    
    # 중단 트랜잭션 실행
    aborted_order_id, abort_result = run_aborted_transaction(producer, order_topic, payment_topic)
    
    # 결과 요약
    print("\n===== 트랜잭션 테스트 결과 =====")
    print(f"✅ 성공 트랜잭션 주문 ID: {success_order_id}")
    print(f"❌ 중단 트랜잭션 주문 ID: {aborted_order_id}")
    
    # 테스트 결과 평가 (성공 트랜잭션은 커밋되고, 중단 트랜잭션은 중단되어야 함)
    if success_result and abort_result:
        print("\n✅ 트랜잭션 프로듀서 테스트 성공")
    else:
        print("\n❌ 트랜잭션 프로듀서 테스트 부분 실패") 