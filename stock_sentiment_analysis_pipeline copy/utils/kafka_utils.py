#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Kafka 유틸리티 모듈
Kafka 연결 및 메시지 생성/소비를 위한 유틸리티 함수들을 제공합니다.
"""

import json
import logging
import sys
import os
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer, errors
from kafka.admin import KafkaAdminClient, NewTopic

# 현재 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.config import KAFKA_CONFIG

logger = logging.getLogger(__name__)

class KafkaHelper:
    """Kafka 연결 및 메시지 관련 헬퍼 클래스"""
    
    def __init__(self):
        """Kafka 헬퍼 초기화"""
        self.config = KAFKA_CONFIG
        print(f"KafkaHelper config: {self.config}")
        self.producer = None
        self.consumer = None
        self.admin_client = None
    
    def create_producer(self):
        """Kafka 프로듀서 생성"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config['bootstrap_servers'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: x.encode('utf-8') if x else None,
                client_id=self.config['client_id'],
                acks='all',  # 모든 복제본의 확인을 기다림
                retries=3,
                request_timeout_ms=15000  # 15초
            )
            logger.info("Kafka 프로듀서가 생성되었습니다.")
            return True
        except errors.KafkaError as e:
            logger.error(f"Kafka 프로듀서 생성 실패: {e}")
            return False
    
    def create_consumer(self, topic, group_id=None):
        """
        Kafka 컨슈머 생성
        
        Args:
            topic (str): 구독할 토픽 이름
            group_id (str, optional): 컨슈머 그룹 ID. 기본값은 None.
        """
        try:
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.config['bootstrap_servers'],
                auto_offset_reset=self.config['auto_offset_reset'],
                enable_auto_commit=self.config['enable_auto_commit'],
                group_id=group_id or self.config['consumer_group_id'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                max_poll_records=self.config['max_poll_records']
            )
            logger.info(f"Kafka 컨슈머가 생성되었습니다. 토픽: {topic}")
            return True
        except errors.KafkaError as e:
            logger.error(f"Kafka 컨슈머 생성 실패: {e}")
            return False
    
    def create_admin_client(self):
        """Kafka 관리자 클라이언트 생성"""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.config['bootstrap_servers'],
                client_id=f"{self.config['client_id']}-admin"
            )
            logger.info("Kafka 관리자 클라이언트가 생성되었습니다.")
            return True
        except errors.KafkaError as e:
            logger.error(f"Kafka 관리자 클라이언트 생성 실패: {e}")
            return False
    
    def ensure_topics_exist(self):
        """필요한 토픽이 존재하는지 확인하고, 없으면 생성"""
        if not self.admin_client:
            if not self.create_admin_client():
                return False
        
        try:
            # 존재하는 토픽 목록 가져오기
            existing_topics = self.admin_client.list_topics()
            logger.info(f"존재하는 토픽: {existing_topics}")
            
            # 생성이 필요한 토픽 확인
            topics_to_create = []
            for topic_key, topic_name in self.config['topics'].items():
                if topic_name not in existing_topics:
                    topics_to_create.append(NewTopic(
                        name=topic_name,
                        num_partitions=3,  # 파티션 수
                        replication_factor=1  # 복제 팩터
                    ))
            
            # 필요한 토픽 생성
            if topics_to_create:
                self.admin_client.create_topics(topics_to_create)
                logger.info(f"다음 토픽이 생성되었습니다: {[t.name for t in topics_to_create]}")
            
            return True
        except errors.KafkaError as e:
            logger.error(f"토픽 생성 실패: {e}")
            return False
        finally:
            if self.admin_client:
                self.admin_client.close()
                self.admin_client = None
    
    def send_message(self, topic, key, value):
        """
        Kafka 토픽에 메시지 전송
        
        Args:
            topic (str): 메시지를 보낼 토픽
            key (str): 메시지 키
            value (dict): 메시지 값 (JSON으로 직렬화 가능한 딕셔너리)
        
        Returns:
            bool: 성공 여부
        """
        if not self.producer:
            if not self.create_producer():
                return False
        
        try:
            # 타임스탬프 추가
            if 'timestamp' not in value:
                value['timestamp'] = datetime.now().isoformat()
            
            # 메시지 전송
            self.producer.send(topic, key=key, value=value)
            self.producer.flush()
            return True
        except Exception as e:
            logger.error(f"메시지 전송 실패: {e}")
            return False
    
    def close(self):
        """Kafka 리소스 정리"""
        if self.producer:
            self.producer.close()
            self.producer = None
        
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None