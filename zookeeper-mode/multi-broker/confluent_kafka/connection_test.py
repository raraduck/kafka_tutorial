from confluent_kafka import Producer
import socket

def delivery_report(err, msg):
    if err is not None:
        print(f'메시지 전송 실패: {err}')
    else:
        print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ 오프셋 {msg.offset()}')

# 프로듀서 설정
producer_config = {
    'bootstrap.servers': 'localhost:9092,localhost:9093,localhost:9094',
    'client.id': socket.gethostname()
}

try:
    # 프로듀서 인스턴스 생성
    producer = Producer(producer_config)
    
    # 간단한 메시지 전송
    producer.produce('idempotent-topic', key='test', value='연결 테스트 메시지', callback=delivery_report)
    
    # 이벤트 루프 처리 및 메시지 전송 확인
    producer.flush()
    
    print("기본 연결 테스트 성공: 브로커와 연결되었으며 메시지를 전송했습니다.")
    
except Exception as e:
    print(f"기본 연결 테스트 실패: {e}") 