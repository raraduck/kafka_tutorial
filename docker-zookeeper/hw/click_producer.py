# 문제 2: 클릭 스트림 데이터 생성 및 Kafka 전송
import time
import json
from kafka import KafkaProducer
from faker import Faker
import random
import logging
import argparse

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092' # Docker 외부에서 접속 시
KAFKA_TOPIC = 'click_stream'
SUSPICIOUS_USER_ID = "suspicious_user_007" # 의심 활동 유도용 사용자 ID

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(f"Starting to send messages to topic: {KAFKA_TOPIC}")

def generate_click_event(fake: Faker, user_id: str | None = None) -> dict:
    """가상의 클릭 이벤트 데이터를 생성합니다. 특정 user_id를 지정할 수 있습니다."""
    event = {
        'user_id': user_id if user_id else fake.uuid4(), # 지정된 user_id 또는 랜덤 UUID
        'page_url': fake.uri(), # "https://google.com/", # fake.uri(),
        'click_time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'referrer': fake.uri() if random.random() > 0.3 else None,
        'user_agent': fake.user_agent()
    }
    return event

def main(duration_seconds: int | None = None):
    """메인 실행 함수. 지정된 시간 동안 또는 무한히 메시지를 생성하여 Kafka로 전송합니다.
       낮은 확률로 의심스러운 활동 패턴을 생성합니다.
    """
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        )
        logging.info(f"Kafka Producer initialized. Target topic: {KAFKA_TOPIC}")
    except Exception as e:
        logging.error(f"Failed to initialize Kafka Producer: {e}")
        return

    fake = Faker()
    start_time = time.time()
    messages_sent_total = 0
    
    logging.info(f"Starting to send messages to Kafka topic: {KAFKA_TOPIC}")
    if duration_seconds:
        logging.info(f"Producer will run for {duration_seconds} seconds.")

    try:
        while True:
            # 시간 제한 로직
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                logging.info(f"Specified duration of {duration_seconds} seconds reached. Stopping producer.")
                break

            # --- 확률적으로 의심 활동 버스트 생성 또는 일반 활동 생성 --- 
            if random.random() < 0.05: # 5% 확률로 의심 활동 버스트 생성
                logging.warning(f"<<< Generating suspicious activity burst for user: {SUSPICIOUS_USER_ID} >>>")
                burst_start_time = time.time()
                burst_messages_sent = 0
                num_burst_messages = random.randint(7, 12) # 짧은 시간에 5개 이상의 고유 페이지 생성 목표
                generated_urls = set() 

                for _ in range(num_burst_messages):
                    if duration_seconds and (time.time() - start_time) >= duration_seconds: break
                    
                    # 항상 새로운 URL 생성 시도
                    click_event = generate_click_event(fake, user_id=SUSPICIOUS_USER_ID)
                    generated_urls.add(click_event['page_url'])

                    try:
                        producer.send(KAFKA_TOPIC, value=click_event)
                        burst_messages_sent += 1
                        messages_sent_total += 1
                    except Exception as e:
                        logging.error(f"Failed to send suspicious burst message: {e}")
                    time.sleep(random.uniform(0.01, 0.05)) # 매우 짧은 간격
                
                burst_duration = time.time() - burst_start_time
                logging.warning(f"<<< Finished suspicious burst: Sent {burst_messages_sent} messages ({len(generated_urls)} unique URLs) for {SUSPICIOUS_USER_ID} in {burst_duration:.2f} seconds >>>")
                # 버스트 후 약간의 휴지 시간 (선택적)
                time.sleep(0.5)
            
            else: # 일반 활동 생성 (기존 로직)
                messages_sent_this_second = 0
                second_start_time = time.time()
                num_messages_per_target_second = random.randint(10, 20)
                
                for _ in range(num_messages_per_target_second):
                    if duration_seconds and (time.time() - start_time) >= duration_seconds: break
                    
                    click_event = generate_click_event(fake)
                    try:
                        producer.send(KAFKA_TOPIC, value=click_event)
                        messages_sent_this_second += 1
                        messages_sent_total += 1
                    except Exception as e:
                        logging.error(f"Failed to send message: {e}")
                    time.sleep(random.uniform(0.01, 0.03))

                current_loop_duration = time.time() - second_start_time
                sleep_time = max(0, 1.0 - current_loop_duration)
                time.sleep(sleep_time)
                
                actual_loop_duration = time.time() - second_start_time
                if actual_loop_duration > 0:
                    actual_rate = messages_sent_this_second / actual_loop_duration
                    logging.info(f"Producer: Sent {messages_sent_this_second} normal messages in the last {actual_loop_duration:.2f} seconds to {KAFKA_TOPIC} (Rate: {actual_rate:.2f} msg/sec). Total sent: {messages_sent_total}")
                else:
                    logging.info(f"Producer: Sent {messages_sent_this_second} normal messages in the last {actual_loop_duration:.2f} seconds to {KAFKA_TOPIC}. Total sent: {messages_sent_total}")

    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt received. Shutting down producer...")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if producer:
            logging.info("Flushing and closing Kafka producer...")
            producer.flush()
            producer.close()
            logging.info("Producer closed.")
        logging.info(f"Total messages sent: {messages_sent_total}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka Click Stream Producer with Suspicious Activity Injection')
    parser.add_argument('--duration', type=int, help='Duration in seconds for the producer to run.')
    args = parser.parse_args()

    main(duration_seconds=args.duration) 