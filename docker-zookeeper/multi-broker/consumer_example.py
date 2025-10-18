#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
import time
import threading
import signal
import sys
from datetime import datetime
import argparse

def json_deserializer(data):
    """바이트를 JSON 객체로 역직렬화하는 함수"""
    if data is None:
        return None
    return json.loads(data.decode('utf-8'))

def format_timestamp(timestamp_str):
    """ISO 형식 타임스탬프를 가독성 있는 형식으로 변환"""
    dt = datetime.fromisoformat(timestamp_str)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def process_order(order_data):
    """주문 데이터를 처리하는 함수"""
    order_id = order_data['order_id']
    user_id = order_data['user_id']
    products_count = len(order_data['products'])
    total_amount = order_data['total_amount']
    payment_method = order_data['payment_method']
    timestamp = format_timestamp(order_data['order_timestamp'])
    
    # 출력 형식 지정
    print(f"\n{'='*70}")
    print(f"📦 주문 정보 (시간: {timestamp})")
    print(f"{'='*70}")
    print(f"🔑 주문 ID: {order_id}")
    print(f"👤 사용자 ID: {user_id}")
    print(f"💰 총 결제액: {total_amount:,}원")
    print(f"💳 결제 수단: {payment_method}")
    print(f"📋 상품 수: {products_count}개")
    
    # 상품 목록 출력
    print("\n📝 주문 상품 목록:")
    for idx, product in enumerate(order_data['products'], 1):
        print(f"  {idx}. {product['product_id']} - {product['price']:,}원 x {product['quantity']}개 = {product['amount']:,}원")
    
    print(f"{'='*70}\n")
    
    # 주문 처리 지연 시뮬레이션 (실제 비즈니스 로직 처리 시간)
    time.sleep(0.5)

class MultiThreadedConsumer:
    """멀티스레드 방식으로 동작하는 Kafka 컨슈머 클래스"""
    
    def __init__(self, bootstrap_servers, topic_name, group_id, 
                 auto_offset_reset='earliest', enable_auto_commit=False):
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.enable_auto_commit = enable_auto_commit
        self.stop_event = threading.Event()
        self.consumers = []
        self.threads = []
        
        # 종료 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
    
    def signal_handler(self, sig, frame):
        """시그널 핸들러 - 안전하게 종료"""
        print("\n종료 신호 수신. 컨슈머 종료 중...")
        self.stop_event.set()
    
    def consumer_thread(self, thread_id):
        """개별 컨슈머 스레드 함수"""
        consumer = KafkaConsumer(
            self.topic_name,
            bootstrap_servers=self.bootstrap_servers,
            # 모든 스레드가 같은 그룹 ID 사용 (리밸런싱 테스트를 위해)
            group_id=self.group_id,
            # 각 스레드별 고유한 클라이언트 ID 설정
            client_id=f"{self.group_id}-client-{thread_id}",
            auto_offset_reset=self.auto_offset_reset,
            enable_auto_commit=self.enable_auto_commit,
            value_deserializer=json_deserializer,
            session_timeout_ms=10000,
            heartbeat_interval_ms=3000,
            max_poll_interval_ms=300000,
            max_poll_records=500
        )
        
        self.consumers.append(consumer)
        
        print(f"컨슈머 스레드 {thread_id} 시작. 그룹: {self.group_id}, 클라이언트 ID: {self.group_id}-client-{thread_id}")
        
        # 할당된 파티션 출력
        for tp in consumer.assignment():
            print(f"스레드 {thread_id}: 파티션 {tp.partition} 할당됨")
        
        # 메시지 소비 루프
        message_count = 0
        try:
            while not self.stop_event.is_set():
                # 메시지 일괄 가져오기 (타임아웃: 1초)
                records = consumer.poll(timeout_ms=1000)
                
                if not records:
                    continue
                
                # 모든 파티션과 레코드 처리
                for tp, messages in records.items():
                    print(f"\n스레드 {thread_id}: 파티션 {tp.partition}에서 {len(messages)}개 메시지 수신")
                    
                    for message in messages:
                        message_count += 1
                        print(f"스레드 {thread_id} - 메시지 {message_count} 처리 중 (파티션: {message.partition}, 오프셋: {message.offset})")
                        
                        # 주문 처리
                        process_order(message.value)
                        
                        # 처리 완료 후 수동 커밋 (enable_auto_commit=False인 경우)
                        if not self.enable_auto_commit:
                            consumer.commit()
        
        except Exception as e:
            print(f"스레드 {thread_id} 오류: {e}")
        finally:
            consumer.close()
            print(f"스레드 {thread_id} 종료. 총 {message_count}개 메시지 처리.")
    
    def start(self, thread_count=3):
        """지정된 수의 컨슈머 스레드 시작"""
        for i in range(thread_count):
            t = threading.Thread(target=self.consumer_thread, args=(i,))
            t.daemon = True
            self.threads.append(t)
            t.start()
        
        # 모든 스레드가 종료될 때까지 대기
        try:
            while any(t.is_alive() for t in self.threads):
                time.sleep(0.1)
        except KeyboardInterrupt:
            print("종료 요청...")
            self.stop_event.set()
        
        # 스레드 조인
        for t in self.threads:
            t.join()
        
        print("모든 컨슈머 스레드 종료 완료")

def main():
    parser = argparse.ArgumentParser(description='Kafka Multi-Broker Consumer 예제')
    parser.add_argument('--threads', type=int, default=3, help='컨슈머 스레드 수 (기본값: 3)')
    parser.add_argument('--auto-commit', action='store_true', help='자동 커밋 활성화 (기본값: 비활성화)')
    parser.add_argument('--reset', choices=['earliest', 'latest'], default='earliest', 
                        help='오프셋 초기화 전략 (earliest 또는 latest)')
    args = parser.parse_args()
    
    # Kafka 브로커 목록 (다중 브로커)
    bootstrap_servers = [
        'localhost:9092',  # broker 1
        'localhost:9093',  # broker 2
        'localhost:9094'   # broker 3
    ]
    
    print("Kafka Multi-Broker Consumer 시작.")
    print(f"연결된 브로커: {', '.join(bootstrap_servers)}")
    print(f"스레드 수: {args.threads}")
    print(f"자동 커밋: {'활성화' if args.auto_commit else '비활성화'}")
    print(f"오프셋 초기화: {args.reset}")
    
    # 멀티스레드 컨슈머 생성 및 시작
    consumer = MultiThreadedConsumer(
        bootstrap_servers=bootstrap_servers,
        topic_name='orders',
        group_id='order_processing_group',
        auto_offset_reset=args.reset,
        enable_auto_commit=args.auto_commit
    )
    
    consumer.start(thread_count=args.threads)

if __name__ == "__main__":
    main() 