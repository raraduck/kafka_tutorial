"""
Kafka 컨슈머 성능 최적화 패턴 예제

이 모듈은 Kafka 컨슈머의 다양한 최적화 패턴을 구현합니다:
1. 배치 처리 패턴 - 메시지를 모아서 한 번에 처리
2. 병렬 처리 패턴 - 스레드 풀을 이용한 동시 처리
3. 리밸런스 핸들링 패턴 - 컨슈머 그룹 리밸런스 시 상태 관리
"""

from concurrent.futures import ThreadPoolExecutor
import threading
import time
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from confluent_kafka import Producer, OFFSET_BEGINNING, OFFSET_END, OFFSET_STORED


# ==================== 1. 배치 처리 패턴 ====================

def batch_processing_pattern(bootstrap_servers, topic, group_id, batch_size=100):
    """
    메시지를 일정 개수만큼 모아서 배치로 처리하는 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 구독할 토픽
        group_id (str): 컨슈머 그룹 ID
        batch_size (int): 한 번에 처리할 메시지 크기
    """
    # 컨슈머 설정
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # 수동 커밋 모드
        'max.poll.interval.ms': 600000,  # 10분
        'session.timeout.ms': 30000,  # 30초
    }
    
    consumer = Consumer(config)
    consumer.subscribe([topic])
    
    try:
        buffer = []
        
        while True:
            # 100ms 동안 데이터 폴링
            messages = consumer.poll(timeout=100)
            
            if messages is None:
                continue
                
            if messages.error():
                if messages.error().code() == KafkaError._PARTITION_EOF:
                    # 파티션의 끝에 도달
                    print(f"파티션 끝에 도달: {messages.topic()}-{messages.partition()}")
                    continue
                else:
                    # 에러 발생
                    print(f"메시지 폴링 중 에러 발생: {messages.error()}")
                    break
            
            # 메시지 처리 및 버퍼에 추가
            buffer.append(messages.value())
            
            # 배치 크기에 도달하면 처리 및 커밋
            if len(buffer) >= batch_size:
                process_message_batch(buffer)
                consumer.commit()  # 현재 오프셋 커밋
                buffer.clear()
                
    except KeyboardInterrupt:
        print("컨슈머 종료 중...")
    finally:
        # 버퍼에 남은 메시지 처리
        if buffer:
            process_message_batch(buffer)
            consumer.commit()
            
        consumer.close()


def process_message_batch(messages):
    """
    메시지 배치를 처리하는 함수
    
    Args:
        messages (list): 처리할 메시지 리스트
    """
    print(f"{len(messages)}개 메시지 배치 처리 시작")
    # 실제 처리 로직 (DB 저장, 계산 등)
    time.sleep(0.5)  # 처리 시간 시뮬레이션
    print(f"배치 처리 완료")


# ==================== 2. 병렬 처리 패턴 ====================

def parallel_processing_pattern(bootstrap_servers, topic, group_id, workers=10):
    """
    스레드 풀을 사용해 메시지를 병렬로 처리하는 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 구독할 토픽
        group_id (str): 컨슈머 그룹 ID
        workers (int): 스레드 풀의 워커 수
    """
    # 컨슈머 설정
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # 수동 커밋 모드
    }
    
    consumer = Consumer(config)
    consumer.subscribe([topic])
    
    # 스레드 풀 생성
    executor = ThreadPoolExecutor(max_workers=workers)
    
    # 오프셋 추적용 딕셔너리
    last_offset = {}
    offset_lock = threading.Lock()
    
    try:
        while True:
            # 메시지 폴링
            message_batch = consumer.poll(timeout=100)
            
            if message_batch is None:
                continue
                
            if message_batch.error():
                if message_batch.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"에러 발생: {message_batch.error()}")
                    break
            
            # 파티션-토픽 식별자
            tp = TopicPartition(message_batch.topic(), message_batch.partition())
            
            # 비동기 처리 제출
            future = executor.submit(process_message, message_batch.value())
            
            # 완료 대기 및 오프셋 업데이트
            if future.result():  # 처리 성공
                with offset_lock:
                    # 파티션별 마지막 처리 오프셋 기록
                    last_offset[tp] = max(
                        last_offset.get(tp, message_batch.offset() - 1),
                        message_batch.offset()
                    )
            
            # 주기적으로 처리된 오프셋 커밋
            if message_batch.offset() % 100 == 0:
                offsets = {
                    tp: last_offset[tp] + 1 
                    for tp in last_offset
                }
                consumer.commit(offsets=offsets)
                
    except KeyboardInterrupt:
        print("컨슈머 종료 중...")
    finally:
        consumer.close()
        executor.shutdown()


def process_message(message):
    """
    단일 메시지를 처리하는 함수
    
    Args:
        message: 처리할 메시지
        
    Returns:
        bool: 처리 성공 여부
    """
    try:
        # 메시지 처리 로직
        print(f"메시지 처리: {message[:30]}...")
        time.sleep(0.1)  # 처리 시간 시뮬레이션
        return True
    except Exception as e:
        print(f"메시지 처리 중 오류: {e}")
        return False


# ==================== 3. 리밸런스 핸들링 패턴 ====================

class RebalanceHandler:
    """컨슈머 리밸런스 처리를 위한 핸들러 클래스"""
    
    def __init__(self, consumer):
        self.consumer = consumer
        self.processing_states = {}  # 파티션별 처리 상태
        
    def on_assign(self, consumer, partitions):
        """
        파티션 할당 시 호출되는 콜백
        """
        print(f"파티션 할당됨: {partitions}")
        
        # 새로 할당된 파티션에 대한 상태 초기화
        for partition in partitions:
            self.processing_states[partition] = {
                'processed_up_to': None,
                'pending_messages': []
            }
    
    def on_revoke(self, consumer, partitions):
        """
        파티션 회수 전 호출되는 콜백
        """
        print(f"파티션 회수됨: {partitions}")
        
        # 현재 처리 중인 작업 완료 및 상태 저장
        for partition in partitions:
            if partition in self.processing_states:
                # 파티션 상태 정리 (예: 임시 상태 저장)
                pending = self.processing_states[partition]['pending_messages']
                if pending:
                    print(f"파티션 {partition}의 처리 중인 {len(pending)}개 메시지 처리 완료")
                    process_message_batch(pending)
                    
                # 파티션 마지막 처리된 오프셋 커밋
                if self.processing_states[partition]['processed_up_to']:
                    offset = self.processing_states[partition]['processed_up_to'] + 1
                    self.consumer.commit(offsets=[
                        TopicPartition(partition.topic, partition.partition, offset)
                    ])
                
                # 상태 삭제
                del self.processing_states[partition]


def rebalance_handling_pattern(bootstrap_servers, topic, group_id):
    """
    컨슈머 그룹 리밸런스를 적절히 처리하는 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 구독할 토픽
        group_id (str): 컨슈머 그룹 ID
    """
    # 컨슈머 설정
    config = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,  # 수동 커밋 모드
        'max.poll.interval.ms': 300000,  # 5분
    }
    
    consumer = Consumer(config)
    
    # 리밸런스 핸들러 생성
    handler = RebalanceHandler(consumer)
    
    # 콜백 설정
    consumer.subscribe(
        [topic], 
        on_assign=handler.on_assign,
        on_revoke=handler.on_revoke
    )
    
    try:
        while True:
            message = consumer.poll(timeout=100)
            
            if message is None:
                continue
                
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"에러 발생: {message.error()}")
                    break
            
            # 파티션 식별자
            tp = TopicPartition(message.topic(), message.partition())
            
            # 메시지 처리
            # 실제로는 여기서 메시지 처리 또는 배치에 추가
            handler.processing_states[tp]['pending_messages'].append(message.value())
            
            # 일정 개수 이상 모이면 처리
            if len(handler.processing_states[tp]['pending_messages']) >= 10:
                pending = handler.processing_states[tp]['pending_messages']
                process_message_batch(pending)
                handler.processing_states[tp]['processed_up_to'] = message.offset()
                handler.processing_states[tp]['pending_messages'] = []
                
                # 오프셋 커밋
                consumer.commit(message=message)
                
    except KeyboardInterrupt:
        print("컨슈머 종료 중...")
    finally:
        consumer.close()


# 메인 함수 - 예제 실행
if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "localhost:9092"
    TOPIC = "test-topic"
    GROUP_ID = "test-group"
    
    print("=== 배치 처리 패턴 테스트 ===")
    # batch_processing_pattern(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)
    
    print("\n=== 병렬 처리 패턴 테스트 ===")
    # parallel_processing_pattern(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)
    
    print("\n=== 리밸런스 처리 패턴 테스트 ===")
    # rebalance_handling_pattern(BOOTSTRAP_SERVERS, TOPIC, GROUP_ID)
    
    print("\n테스트하려면 원하는 패턴의 주석을 제거하고 실행하세요.") 