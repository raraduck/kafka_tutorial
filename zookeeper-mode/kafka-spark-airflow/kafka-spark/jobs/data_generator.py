"""
Kafka 토픽에 테스트 데이터를 생성하는 스크립트

이 스크립트는 설정된 주기마다 Kafka 토픽에 샘플 데이터를 생성합니다.
실제 환경의 IoT 디바이스 데이터를 시뮬레이션합니다.
"""
import json
import time
import random
import datetime
from confluent_kafka import Producer
import argparse


def generate_data(device_id):
    """
    각 디바이스에 대한 샘플 데이터를 생성합니다.
    가끔 이상값을 생성하여 이상 감지 알림을 트리거합니다.
    """
    timestamp = datetime.datetime.now().isoformat()
    
    # 10% 확률로 이상값 생성 (200~500 범위의 높은 값)
    if random.random() < 0.10:
        value = round(random.uniform(200, 500), 2)
        status = random.choices(["Danger", "Error"], weights=[0.6, 0.4], k=1)[0]
    else:
        value = round(random.uniform(10, 100), 2)
        status_options = ["Normal", "Warning", "Danger", "Error"]
        # 정상 상태가 나올 확률이 높게 가중치 설정
        status = random.choices(status_options, weights=[0.7, 0.15, 0.1, 0.05], k=1)[0]
    
    return {
        "timestamp": timestamp,
        "value": value,
        "device_id": device_id,
        "status": status
    }


def delivery_report(err, msg):
    """
    Kafka 메시지 전송 결과를 처리하는 콜백 함수
    """
    if err is not None:
        print(f'메시지 전송 실패: {err}')
    else:
        print(f'메시지 전송 성공: {msg.topic()} [{msg.partition()}] @ {msg.offset()}')


def produce_messages(producer, topic, num_devices, interval, max_messages=None):
    """
    지정된 수의 디바이스에 대한 메시지를 생성하고 Kafka에 전송합니다.
    """
    device_ids = [f"device_{i:03d}" for i in range(1, num_devices + 1)]
    count = 0
    
    try:
        while max_messages is None or count < max_messages:
            for device_id in device_ids:
                data = generate_data(device_id)
                # 메시지 키를 디바이스 ID로 설정 (같은 디바이스는 같은 파티션에 할당)
                producer.produce(
                    topic=topic,
                    key=device_id,
                    value=json.dumps(data),
                    callback=delivery_report
                )
                
                count += 1
                if count % 100 == 0:
                    # 100개 메시지마다 버퍼 비우기
                    producer.flush()
            
            # 인터벌 대기
            time.sleep(interval)
            
            # max_messages 설정 시 진행 상황 출력
            if max_messages is not None:
                print(f"진행: {count}/{max_messages} 메시지 생성됨 ({count/max_messages*100:.1f}%)")
                
    except KeyboardInterrupt:
        print("사용자에 의해 중단됨")
    finally:
        # 남은 메시지 전송 보장
        producer.flush()
        print(f"총 {count}개 메시지가 생성되어 {topic} 토픽으로 전송됨")


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='Kafka에 테스트 데이터 생성')
    parser.add_argument('--topic', type=str, default='stream_data_topic',
                        help='데이터를 생성할 Kafka 토픽')
    parser.add_argument('--bootstrap-servers', type=str, default='kafka:29092',
                        help='Kafka 브로커 주소')
    parser.add_argument('--num-devices', type=int, default=5,
                        help='시뮬레이션할 디바이스 수')
    parser.add_argument('--interval', type=float, default=1.0,
                        help='데이터 생성 간격(초)')
    parser.add_argument('--max-messages', type=int, default=None,
                        help='생성할 최대 메시지 수 (None=무제한)')
    
    args = parser.parse_args()
    
    # Kafka 프로듀서 설정
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'data-generator'
    }
    
    producer = Producer(conf)
    
    print(f"데이터 생성 시작 - 토픽: {args.topic}, 디바이스 수: {args.num_devices}, 간격: {args.interval}초")
    produce_messages(producer, args.topic, args.num_devices, args.interval, args.max_messages)


if __name__ == "__main__":
    main() 