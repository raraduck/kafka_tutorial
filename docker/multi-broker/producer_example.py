#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaProducer
import json
import time
import random
import sys
import uuid
from datetime import datetime

def json_serializer(data):
    """데이터를 JSON 형식으로 직렬화하는 함수"""
    return json.dumps(data).encode('utf-8')

def get_partition_key(user_id):
    """사용자 ID를 기반으로 파티션 키를 생성하는 함수"""
    return str(user_id).encode('utf-8')  # 문자열로 변환 후 바이트로 인코딩

def generate_order_data():
    """주문 데이터를 생성하는 함수"""
    user_id = random.randint(1, 1000)
    order_id = str(uuid.uuid4())
    products = []
    total_amount = 0
    
    # 1~5개 사이의 랜덤한 상품 생성
    for _ in range(random.randint(1, 5)):
        product_id = f"PROD-{random.randint(1000, 9999)}"
        price = random.randint(1000, 50000)
        quantity = random.randint(1, 10)
        amount = price * quantity
        total_amount += amount
        
        products.append({
            "product_id": product_id,
            "price": price,
            "quantity": quantity,
            "amount": amount
        })
    
    return {
        "order_id": order_id,
        "user_id": user_id,
        "order_timestamp": datetime.now().isoformat(),
        "products": products,
        "total_amount": total_amount,
        "payment_method": random.choice(["credit_card", "bank_transfer", "mobile_payment"]),
        "shipping_address": f"Address-{random.randint(1, 1000)}"
    }

def on_send_success(record_metadata):
    """메시지 전송 성공 시 호출되는 콜백 함수"""
    print(f"메시지 전송 성공 - 토픽: {record_metadata.topic}, 파티션: {record_metadata.partition}, 오프셋: {record_metadata.offset}")

def on_send_error(excp):
    """메시지 전송 실패 시 호출되는 콜백 함수"""
    print(f"메시지 전송 실패: {excp}")

def main():
    # Kafka 브로커 목록 (다중 브로커)
    bootstrap_servers = [
        'localhost:9092',  # broker 1
        'localhost:9093',  # broker 2
        'localhost:9094'   # broker 3
    ]
    
    # Kafka Producer 생성 - 고급 설정
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,  # 다중 브로커 설정
        value_serializer=json_serializer,     # 값 직렬화 함수
        key_serializer=lambda k: str(k).encode('utf-8'),  # 키 직렬화 함수
        acks='all',                           # 모든 복제본 확인
        retries=5,                            # 실패 시 재시도 횟수
        linger_ms=20,                         # 배치 처리를 위한 대기 시간 (ms)
        batch_size=16384,                     # 배치 크기 (바이트)
        buffer_memory=33554432,               # 버퍼 메모리 (32MB)
        compression_type='gzip',              # 압축 유형 (none, gzip, snappy, lz4)
        max_in_flight_requests_per_connection=1   # 연결당 최대 인플라이트 요청 수
    )

    print("Kafka Multi-Broker Producer 시작. Ctrl+C로 종료.")
    print(f"연결된 브로커: {', '.join(bootstrap_servers)}")

    # 토픽 이름
    topic_name = 'orders'
    
    try:
        # 메시지 전송 루프
        message_count = 0
        while True:
            # 주문 데이터 생성
            order_data = generate_order_data()
            user_id = order_data['user_id']
            
            # 메시지 전송 (키 기반 파티셔닝)
            future = producer.send(
                topic=topic_name,
                key=user_id,  # 사용자 ID를 키로 사용 (같은 사용자의 주문은 같은 파티션으로)
                value=order_data
            )
            
            # 콜백 등록
            future.add_callback(on_send_success).add_errback(on_send_error)
            
            # 진행 상황 출력
            message_count += 1
            if message_count % 5 == 0:
                print(f"\n========== {message_count}개 메시지 전송 완료 ==========")
            print(f"주문 메시지 전송: Order ID: {order_data['order_id']}, User ID: {user_id}, 금액: {order_data['total_amount']}원")
            
            # 전송 간격
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\n프로듀서 종료")
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        # 남은 메시지 전송 및 리소스 해제
        producer.flush()
        producer.close()
        print("프로듀서 리소스 해제 완료")

if __name__ == "__main__":
    main() 