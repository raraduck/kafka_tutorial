"""
Kafka Producer 활용 패턴 예제

이 모듈은 Kafka Producer를, 다양한 상황에 맞게 활용하는 패턴을 제공합니다:
1. 배치 전송 패턴 - 메시지를 모아서 배치로 전송
2. 비동기 전송 패턴 - 콜백 함수 활용
3. 키 기반 파티셔닝 - 특정 키를 가진 메시지를 같은 파티션에 전송
4. 멱등성 구현 - 중복 전송 방지
5. 스키마 레지스트리 활용 - Avro 스키마 사용 예제
"""

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import time
import json
import random
import threading
import uuid
import socket
import datetime

# ==================== 1. 기본 Producer 생성 함수 ====================

def create_producer(bootstrap_servers, client_id=None, acks='all'):
    """
    기본 Producer 인스턴스 생성
    
    Args:
        bootstrap_servers (str): Kafka 브로커 서버 주소
        client_id (str): Producer 클라이언트 ID (기본값: 호스트명)
        acks (str): acks 설정 ('0', '1', 'all')
        
    Returns:
        Producer: 생성된 Producer 인스턴스
    """
    if client_id is None:
        client_id = f"producer-{socket.gethostname()}"
        
    config = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': client_id,
        'acks': acks
    }
    
    return Producer(config)


def delivery_report(err, msg):
    """
    메시지 전송 결과를 처리하는 콜백 함수
    
    Args:
        err: 오류 정보 (성공 시 None)
        msg: 전송된 메시지 객체
    """
    if err is not None:
        print(f'메시지 전송 실패: {err}')
    else:
        print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset()}')


# ==================== 2. 배치 전송 패턴 ====================

def batch_send_pattern(bootstrap_servers, topic, batch_size=100, linger_ms=100):
    """
    메시지를 배치로 전송하는 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 대상 토픽 이름
        batch_size (int): 배치 크기 (default: 100)
        linger_ms (int): 배치 전송 대기 시간 (ms)
    """
    # 배치 처리를 위한 Producer 설정
    config = {
        'bootstrap.servers': bootstrap_servers,
        'batch.size': batch_size,  # 배치 크기 설정
        'linger.ms': linger_ms,    # 배치 모음 대기 시간
        'acks': 'all'
    }
    
    producer = Producer(config)
    
    messages_sent = 0
    start_time = time.time()
    
    try:
        # 1000개 메시지 배치 전송 테스트
        for i in range(1000):
            data = {
                'id': i,
                'timestamp': datetime.datetime.now().isoformat(),
                'value': f"batch-message-{i}"
            }
            
            # 메시지를 JSON 형태로 직렬화
            producer.produce(
                topic=topic,
                key=str(i),
                value=json.dumps(data).encode('utf-8'),
                callback=delivery_report
            )
            
            messages_sent += 1
            
            # 배치 전송 중간에 poll()을 호출하여 콜백 실행
            if i % 100 == 0:
                producer.poll(0)
                
        # 남은 메시지 처리
        producer.flush()
        
        end_time = time.time()
        elapsed = end_time - start_time
        messages_per_second = messages_sent / elapsed
        
        print(f"\n배치 전송 성능:")
        print(f"  전송 메시지 수: {messages_sent}")
        print(f"  소요 시간: {elapsed:.2f}초")
        print(f"  초당 메시지 수: {messages_per_second:.2f}/s")
        
    except KeyboardInterrupt:
        print("사용자에 의해 중단됨")
        producer.flush(10)
    except Exception as e:
        print(f"오류 발생: {e}")
        producer.flush(10)


# ==================== 3. 비동기 전송 패턴 ====================

def async_send_pattern(bootstrap_servers, topic, message_count=100):
    """
    메시지를 비동기적으로 전송하는 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 대상 토픽 이름
        message_count (int): 전송할 메시지 수
    """
    config = {
        'bootstrap.servers': bootstrap_servers,
        'acks': 'all'
    }
    
    producer = Producer(config)
    sent_count = 0
    success_count = 0
    error_count = 0
    
    # 전송 결과를 추적하는 콜백 함수
    def on_delivery(err, msg):
        nonlocal success_count, error_count
        if err:
            print(f'메시지 전송 실패: {err}')
            error_count += 1
        else:
            success_count += 1
    
    print(f"{message_count}개 메시지 비동기 전송 시작...")
    start_time = time.time()
    
    try:
        for i in range(message_count):
            data = {
                'id': i,
                'timestamp': datetime.datetime.now().isoformat(),
                'message': f"async-message-{i}"
            }
            
            producer.produce(
                topic=topic,
                key=str(i),
                value=json.dumps(data).encode('utf-8'),
                callback=on_delivery
            )
            sent_count += 1
            
            # 주기적으로 poll 호출하여 이벤트 처리
            producer.poll(0)
            
        # 완료 대기
        remaining = producer.flush(10)
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n비동기 전송 결과:")
        print(f"  전송 시도: {sent_count}")
        print(f"  전송 성공: {success_count}")
        print(f"  전송 실패: {error_count}")
        print(f"  미완료 메시지: {remaining}")
        print(f"  소요 시간: {elapsed:.2f}초")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        producer.flush(10)


# ==================== 4. 키 기반 파티셔닝 패턴 ====================

def key_based_partitioning(bootstrap_servers, topic, keyed_data):
    """
    키 기반으로 파티셔닝하여 메시지 전송
    같은 키를 가진 메시지는 항상 같은 파티션으로 전송됨
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 대상 토픽 이름
        keyed_data (dict): 키-값 형태의 데이터 목록
    """
    producer = create_producer(bootstrap_servers)
    
    print(f"{len(keyed_data)}개 메시지를 키 기반으로 전송 중...")
    
    partition_stats = {}
    
    # 전송 결과 추적 함수
    def key_partitioning_callback(err, msg):
        if err:
            print(f'메시지 전송 실패: {err}')
        else:
            key = msg.key().decode('utf-8')
            partition = msg.partition()
            
            # 파티션별 통계 업데이트
            if partition not in partition_stats:
                partition_stats[partition] = {}
                
            if key not in partition_stats[partition]:
                partition_stats[partition][key] = 0
                
            partition_stats[partition][key] += 1
    
    try:
        # 키가 지정된 메시지 전송
        for key, messages in keyed_data.items():
            for message in messages:
                producer.produce(
                    topic=topic,
                    key=key.encode('utf-8'),
                    value=json.dumps(message).encode('utf-8'),
                    callback=key_partitioning_callback
                )
                
                producer.poll(0)
        
        producer.flush()
        
        # 파티션 분포 출력
        print("\n키-파티션 분포 결과:")
        for partition, keys in partition_stats.items():
            print(f"  파티션 {partition}:")
            for key, count in keys.items():
                print(f"    키 '{key}': {count}개 메시지")
                
    except Exception as e:
        print(f"오류 발생: {e}")
        producer.flush()


# ==================== 5. 멱등성 Producer 패턴 ====================

def idempotent_producer_pattern(bootstrap_servers, topic, message_count=100):
    """
    멱등성을 보장하는 Producer 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 대상 토픽 이름
        message_count (int): 전송할 메시지 수
    """
    # 멱등성 설정을 포함한 Producer 구성
    config = {
        'bootstrap.servers': bootstrap_servers,
        'acks': 'all',
        'enable.idempotence': True,  # 멱등성 활성화
        # 멱등성을 위한 설정
        'max.in.flight.requests.per.connection': 5,
        'retries': 5,
        'linger.ms': 5
    }
    
    producer = Producer(config)
    print(f"멱등성 Producer로 {message_count}개 메시지 전송 중...")
    
    # 간단한 전송 결과 추적
    successful = 0
    
    def on_idempotent_delivery(err, msg):
        nonlocal successful
        if err:
            print(f'멱등성 전송 실패: {err}')
        else:
            successful += 1
    
    # 멱등성 테스트를 위한 전송 함수
    def send_with_retry(message_data, retries=3):
        """특정 메시지를 여러 번 전송 시도"""
        for _ in range(retries):
            try:
                producer.produce(
                    topic=topic,
                    key=message_data['id'].encode('utf-8'),
                    value=json.dumps(message_data).encode('utf-8'),
                    callback=on_idempotent_delivery
                )
                producer.poll(0)
                # 전송 직후 일부러 지연 추가
                time.sleep(0.01)
            except BufferError:
                # 버퍼가 가득 찬 경우 잠시 대기
                producer.poll(0.5)
        
    try:
        # 테스트 메시지 생성 및 전송
        for i in range(message_count):
            message_id = str(uuid.uuid4())
            message_data = {
                'id': message_id,
                'timestamp': datetime.datetime.now().isoformat(),
                'value': f"idempotent-msg-{i}"
            }
            
            # 동일 메시지를 반복 전송 (멱등성 테스트)
            if i % 10 == 0:
                print(f"메시지 {i}는 중복 전송을 시뮬레이션합니다...")
                send_with_retry(message_data, retries=3)
            else:
                producer.produce(
                    topic=topic,
                    key=message_id.encode('utf-8'),
                    value=json.dumps(message_data).encode('utf-8'),
                    callback=on_idempotent_delivery
                )
            
            # 주기적 poll
            if i % 20 == 0:
                producer.poll(0)
        
        # 모든 메시지 전송 완료 대기
        producer.flush()
        
        print(f"\n멱등성 전송 결과:")
        print(f"  전송 성공: {successful}/{message_count} 메시지")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        producer.flush()


# ==================== 6. Avro 스키마 사용 패턴 (스키마 레지스트리 필요) ====================

def avro_producer_pattern(bootstrap_servers, schema_registry_url, topic):
    """
    Avro 스키마를 사용하는 Producer 패턴
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        schema_registry_url (str): 스키마 레지스트리 URL
        topic (str): 대상 토픽 이름
    """
    # 스키마 레지스트리 클라이언트 생성
    schema_registry_conf = {'url': schema_registry_url}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
    # Avro 스키마 정의
    user_schema_str = """
    {
       "namespace": "example.avro",
       "type": "record",
       "name": "User",
       "fields": [
         {"name": "id", "type": "int"},
         {"name": "name", "type": "string"},
         {"name": "email", "type": "string"},
         {"name": "created_at", "type": "string"}
       ]
    }
    """
    
    # AvroSerializer 생성
    avro_serializer = AvroSerializer(schema_registry_client, user_schema_str)
    string_serializer = StringSerializer('utf_8')
    
    # Producer 구성
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'acks': 'all'
    }
    
    producer = Producer(producer_conf)
    
    # 전송 결과 콜백
    def avro_delivery_report(err, msg):
        if err is not None:
            print(f'Avro 메시지 전송 실패: {err}')
        else:
            print(f'Avro 메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset()}')
    
    # 테스트 데이터 생성
    users = [
        {"id": 1, "name": "홍길동", "email": "hong@example.com", "created_at": datetime.datetime.now().isoformat()},
        {"id": 2, "name": "김철수", "email": "kim@example.com", "created_at": datetime.datetime.now().isoformat()},
        {"id": 3, "name": "이영희", "email": "lee@example.com", "created_at": datetime.datetime.now().isoformat()},
    ]
    
    print(f"Avro 스키마를 사용하여 {len(users)}명의 사용자 데이터 전송 중...")
    
    try:
        # Avro 형식으로 직렬화하여 메시지 전송
        for user in users:
            producer.produce(
                topic=topic,
                key=string_serializer(f"user-{user['id']}"),
                value=avro_serializer(user),
                callback=avro_delivery_report
            )
            producer.poll(0)
        
        # 전송 완료 대기
        producer.flush()
        print("Avro 메시지 전송 완료")
        
    except Exception as e:
        print(f"Avro 전송 중 오류 발생: {e}")
        producer.flush()


# ==================== 7. 트랜잭션 Producer 패턴 ====================

def transactional_producer_pattern(bootstrap_servers, topic, message_count=10):
    """
    트랜잭션을 사용하는 Producer 패턴
    정확히 한 번(exactly-once) 처리 의미론 제공
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        topic (str): 대상 토픽 이름
        message_count (int): 트랜잭션당 메시지 수
    """
    # 트랜잭션 Producer 설정
    transactional_id = f"transaction-producer-{uuid.uuid4()}"
    config = {
        'bootstrap.servers': bootstrap_servers,
        'transactional.id': transactional_id,  # 트랜잭션 ID 설정
        'acks': 'all',
        'enable.idempotence': True  # 트랜잭션에는 멱등성이 필요함
    }
    
    producer = Producer(config)
    
    # Producer 트랜잭션 초기화
    producer.init_transactions()
    
    print(f"트랜잭션 Producer 초기화 완료 (ID: {transactional_id})")
    print(f"{message_count}개 메시지를 포함하는 3개 트랜잭션 실행...")
    
    try:
        # 트랜잭션 1: 정상 커밋
        producer.begin_transaction()
        print("\n트랜잭션 #1 시작...")
        
        for i in range(message_count):
            data = {
                'transaction': 1,
                'message_id': i,
                'timestamp': datetime.datetime.now().isoformat(),
                'value': f"tx1-message-{i}"
            }
            
            producer.produce(
                topic=topic,
                key=f"tx1-{i}".encode('utf-8'),
                value=json.dumps(data).encode('utf-8')
            )
            producer.poll(0)
        
        # 트랜잭션 커밋
        producer.commit_transaction()
        print("트랜잭션 #1 커밋 완료")
        
        # 트랜잭션 2: 일부러 중단
        producer.begin_transaction()
        print("\n트랜잭션 #2 시작...")
        
        for i in range(message_count):
            data = {
                'transaction': 2,
                'message_id': i,
                'timestamp': datetime.datetime.now().isoformat(),
                'value': f"tx2-message-{i}"
            }
            
            producer.produce(
                topic=topic,
                key=f"tx2-{i}".encode('utf-8'),
                value=json.dumps(data).encode('utf-8')
            )
            producer.poll(0)
            
            # 4번째 메시지에서 일부러 중단
            if i == 3:
                print("트랜잭션 #2 중간 중단 (의도적)")
                producer.abort_transaction()
                print("트랜잭션 #2 중단 완료")
                break
        
        # 트랜잭션 3: 정상 커밋
        producer.begin_transaction()
        print("\n트랜잭션 #3 시작...")
        
        for i in range(message_count):
            data = {
                'transaction': 3,
                'message_id': i,
                'timestamp': datetime.datetime.now().isoformat(),
                'value': f"tx3-message-{i}"
            }
            
            producer.produce(
                topic=topic,
                key=f"tx3-{i}".encode('utf-8'),
                value=json.dumps(data).encode('utf-8')
            )
            producer.poll(0)
        
        # 트랜잭션 커밋
        producer.commit_transaction()
        print("트랜잭션 #3 커밋 완료")
        
        print("\n트랜잭션 테스트 완료!")
        print("결과를 확인하려면 트랜잭션 Consumer로 확인하세요.")
        
    except KafkaException as e:
        print(f"트랜잭션 오류: {e}")
        try:
            producer.abort_transaction()
        except:
            pass
    except Exception as e:
        print(f"일반 오류: {e}")
        try:
            producer.abort_transaction()
        except:
            pass


# ==================== 테스트 및 실행 함수 ====================

def run_key_partition_test(bootstrap_servers, topic):
    """
    키 기반 파티셔닝 테스트 함수
    """
    # 키별 데이터 생성
    keyed_data = {
        "user_1": [
            {"user_id": "user_1", "action": "login", "time": datetime.datetime.now().isoformat()},
            {"user_id": "user_1", "action": "view_item", "time": datetime.datetime.now().isoformat()},
            {"user_id": "user_1", "action": "add_to_cart", "time": datetime.datetime.now().isoformat()}
        ],
        "user_2": [
            {"user_id": "user_2", "action": "login", "time": datetime.datetime.now().isoformat()},
            {"user_id": "user_2", "action": "search", "time": datetime.datetime.now().isoformat()}
        ],
        "user_3": [
            {"user_id": "user_3", "action": "login", "time": datetime.datetime.now().isoformat()},
            {"user_id": "user_3", "action": "logout", "time": datetime.datetime.now().isoformat()}
        ],
        "user_4": [
            {"user_id": "user_4", "action": "register", "time": datetime.datetime.now().isoformat()}
        ]
    }
    
    key_based_partitioning(bootstrap_servers, topic, keyed_data)


def run_all_tests(bootstrap_servers, schema_registry_url=None):
    """
    모든 패턴을 테스트하는 함수
    
    Args:
        bootstrap_servers (str): Kafka 브로커 주소
        schema_registry_url (str): 스키마 레지스트리 URL (선택적)
    """
    # 테스트용 토픽
    batch_topic = "producer-batch-test"
    async_topic = "producer-async-test"
    key_topic = "producer-key-test"
    idempotent_topic = "producer-idempotent-test"
    transaction_topic = "producer-transaction-test"
    avro_topic = "producer-avro-test"
    
    print("=============================================")
    print("Kafka Producer 패턴 테스트 시작")
    print("=============================================")
    
    # 1. 배치 전송 패턴 테스트
    print("\n1. 배치 전송 패턴 테스트")
    batch_send_pattern(bootstrap_servers, batch_topic, batch_size=100, linger_ms=100)
    
    # 2. 비동기 전송 패턴 테스트
    print("\n2. 비동기 전송 패턴 테스트")
    async_send_pattern(bootstrap_servers, async_topic, message_count=50)
    
    # 3. 키 기반 파티셔닝 테스트
    print("\n3. 키 기반 파티셔닝 테스트")
    run_key_partition_test(bootstrap_servers, key_topic)
    
    # 4. 멱등성 Producer 테스트
    print("\n4. 멱등성 Producer 테스트")
    idempotent_producer_pattern(bootstrap_servers, idempotent_topic, message_count=30)
    
    # 5. 트랜잭션 Producer 테스트
    print("\n5. 트랜잭션 Producer 테스트")
    transactional_producer_pattern(bootstrap_servers, transaction_topic, message_count=5)
    
    # 6. Avro 스키마 테스트 (스키마 레지스트리가 있는 경우)
    if schema_registry_url:
        print("\n6. Avro 스키마 Producer 테스트")
        avro_producer_pattern(bootstrap_servers, schema_registry_url, avro_topic)
    
    print("\n=============================================")
    print("모든 Producer 패턴 테스트 완료")
    print("=============================================")


# 메인 함수
if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "localhost:9092"
    # 스키마 레지스트리 URL (선택적)
    SCHEMA_REGISTRY_URL = "http://localhost:8081"
    
    # 모든 테스트 실행하기
    # run_all_tests(BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL)
    
    # 또는 개별 패턴 테스트
    topic = "producer-test-topic"
    
    # 배치 전송 테스트
    # batch_send_pattern(BOOTSTRAP_SERVERS, topic)
    
    # 비동기 전송 테스트
    # async_send_pattern(BOOTSTRAP_SERVERS, topic)
    
    # 키 기반 파티셔닝 테스트
    # run_key_partition_test(BOOTSTRAP_SERVERS, topic)
    
    # 멱등성 Producer 테스트
    # idempotent_producer_pattern(BOOTSTRAP_SERVERS, topic)
    
    # 트랜잭션 Producer 테스트
    # transactional_producer_pattern(BOOTSTRAP_SERVERS, topic)
    
    print("테스트하려면 원하는 함수의 주석을 해제하고 실행하세요.") 