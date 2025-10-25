#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka 유틸리티 모듈
Kafka Producer 및 Consumer 생성과 관련된 유틸리티 함수들을 제공합니다.
"""

import json
import logging
import sys
import os

# 상위 디렉토리를 path에 추가하여 다른 모듈을 import할 수 있도록 함
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.config import KAFKA_BOOTSTRAP_SERVERS

# 로깅 설정
logger = logging.getLogger("kafka_utils")

def create_producer():
    """Kafka Producer 생성"""
    try:
        from kafka import KafkaProducer
        
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info(f"Kafka Producer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}")
        return producer
    except Exception as e:
        logger.error(f"Kafka Producer 연결 실패: {e}")
        return None

def create_consumer(topic, group_id=None, auto_offset_reset='latest', enable_auto_commit=True):
    """Kafka Consumer 생성"""
    try:
        from kafka import KafkaConsumer
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            group_id=group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logger.info(f"Kafka Consumer 연결 성공: {KAFKA_BOOTSTRAP_SERVERS}, 토픽: {topic}")
        return consumer
    except Exception as e:
        logger.error(f"Kafka Consumer 연결 실패: {e}")
        return None

def send_message(producer, topic, message):
    """Kafka 토픽에 메시지 전송"""
    if not producer:
        logger.error("Producer가 없습니다. 메시지를 전송할 수 없습니다.")
        return False, "Producer가 없습니다."
    
    try:
        future = producer.send(topic, message)
        record_metadata = future.get(timeout=10)
        logger.debug(f"메시지 전송 성공: {topic}, 파티션: {record_metadata.partition}, 오프셋: {record_metadata.offset}")
        return True, record_metadata
    except Exception as e:
        logger.error(f"메시지 전송 실패: {e}")
        return False, str(e)

def consume_messages(consumer, timeout_ms=1000):
    """Kafka 토픽에서 메시지 소비"""
    if not consumer:
        logger.error("Consumer가 없습니다. 메시지를 소비할 수 없습니다.")
        return []
    
    try:
        # 메시지 폴링
        message_dict = consumer.poll(timeout_ms=timeout_ms)
        
        messages = []
        for tp, records in message_dict.items():
            for record in records:
                messages.append(record.value)
        
        return messages
    except Exception as e:
        logger.error(f"메시지 소비 실패: {e}")
        return []

def close_kafka_client(client):
    """Kafka 클라이언트(Producer 또는 Consumer) 종료"""
    if client:
        try:
            client.close()
            logger.info("Kafka 클라이언트 종료 완료")
            return True
        except Exception as e:
            logger.error(f"Kafka 클라이언트 종료 실패: {e}")
            return False
    return False

# 테스트 코드
if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 테스트 토픽
    test_topic = "test-topic"
    
    # Producer 생성 및 메시지 전송 테스트
    producer = create_producer()
    if producer:
        success, result = send_message(producer, test_topic, {"test": "message", "timestamp": "2023-01-01T00:00:00"})
        print(f"메시지 전송 결과: {success}, {result}")
        close_kafka_client(producer)
    
    # Consumer 생성 및 메시지 소비 테스트
    consumer = create_consumer(test_topic, "test-group")
    if consumer:
        messages = consume_messages(consumer)
        print(f"수신된 메시지: {messages}")
        close_kafka_client(consumer)