"""
### Kafka에 메시지를 생성하고 소비하는 DAG
이 DAG는 Kafka 토픽에 반려동물 상태 메시지를 생성하고 소비합니다.
"""
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import random

YOUR_NAME = "알렌"
YOUR_PET_NAME = "뭉이"
NUMBER_OF_TREATS = 5
KAFKA_TOPIC = "my_topic"

def prod_function(num_treats, pet_name):
    """특정 수(`num_treats`)의 메시지를 생성합니다. 각 메시지는 반려동물 이름, 간식 후 기분,
    시리즈의 마지막 간식인지 여부를 포함합니다."""
    for i in range(num_treats):
        final_treat = False
        pet_mood_post_treat = random.choices(
            ["satisfied", "happy", "zoomy", "bouncy"], weights=[2, 2, 1, 1], k=1
        )[0]
        if i + 1 == num_treats:
            final_treat = True
        yield (
            json.dumps(i),
            json.dumps(
                {
                    "pet_name": pet_name,
                    "pet_mood_post_treat": pet_mood_post_treat,
                    "final_treat": final_treat,
                }
            ),
        )

def consume_function(message, owner_name=None):
    """소비된 메시지를 받아 로그에 출력합니다."""
    try:
        # 디버그 정보 출력
        print(f"메시지 유형: {type(message)}")
        
        # owner_name 파라미터가 메시지로 전달된 경우 (인자 순서 문제)
        if owner_name is None and isinstance(message, str) and not message.startswith('{'):
            owner_name = message
            print(f"첫 번째 인자가 메시지가 아닌 소유자 이름({owner_name})으로 전달되었습니다.")
            return True
        
        # cimpl.Message 객체 처리 (confluent-kafka 라이브러리)
        if hasattr(message, 'value') and callable(getattr(message, 'value', None)):
            try:
                message_value = message.value()
                if message_value is None:
                    print("메시지 값이 None입니다.")
                    return True
                
                if isinstance(message_value, bytes):
                    message_content = json.loads(message_value.decode('utf-8'))
                else:
                    message_content = json.loads(message_value)
                
                key = None
                if hasattr(message, 'key') and callable(getattr(message, 'key', None)):
                    key_value = message.key()
                    if key_value:
                        if isinstance(key_value, bytes):
                            key = json.loads(key_value.decode('utf-8'))
                        else:
                            key = json.loads(key_value)
                
                pet_name = message_content.get("pet_name", "알 수 없음")
                pet_mood_post_treat = message_content.get("pet_mood_post_treat", "알 수 없음")
                is_final = message_content.get("final_treat", False)
                
                if key is not None:
                    print(f"메시지 #{key}: 안녕하세요 {owner_name or '주인'}님, 반려동물 {pet_name}이(가) 간식을 먹고 지금 {pet_mood_post_treat} 상태입니다!")
                else:
                    print(f"메시지: 안녕하세요 {owner_name or '주인'}님, 반려동물 {pet_name}이(가) 간식을 먹고 지금 {pet_mood_post_treat} 상태입니다!")
                
                print(f"마지막 간식입니까? {is_final}")
                return True
            except Exception as e:
                print(f"cimpl.Message 객체 처리 중 오류 발생: {e}")
                return True
                
        # Airflow 2.8.1+에서는 메시지가 문자열로 전달됨
        elif isinstance(message, str):
            try:
                message_content = json.loads(message)
                pet_name = message_content.get("pet_name", "알 수 없음")
                pet_mood_post_treat = message_content.get("pet_mood_post_treat", "알 수 없음")
                is_final = message_content.get("final_treat", False)
                print(
                    f"메시지: 안녕하세요 {owner_name or '주인'}님, 반려동물 {pet_name}이(가) 간식을 먹고 지금 {pet_mood_post_treat} 상태입니다!"
                )
                print(f"마지막 간식입니까? {is_final}")
            except json.JSONDecodeError:
                print(f"JSON이 아닌 문자열 메시지를 받았습니다: {message}")
                return True
            return True
        # 알 수 없는 메시지 형식
        else:
            print(f"지원되지 않는 메시지 형식: {type(message)}")
            print(f"메시지 내용: {message}")
            return True
    except Exception as e:
        print(f"메시지 처리 중 오류 발생: {e}")
        print(f"메시지 내용: {message}")
        if owner_name:
            print(f"소유자 이름: {owner_name}")
        return False

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["kafka", "example"],
)
def produce_consume_treats():
    @task
    def get_your_pet_name(pet_name=None):
        return pet_name

    @task
    def get_number_of_treats(num_treats=None):
        return num_treats

    @task
    def get_pet_owner_name(your_name=None):
        return your_name

    produce_treats = ProduceToTopicOperator(
        task_id="produce_treats",
        kafka_config_id="kafka_default",
        topic=KAFKA_TOPIC,
        producer_function="produce_consume_treats.prod_function",
        producer_function_args=[
            "{{ ti.xcom_pull(task_ids='get_number_of_treats') }}",
            "{{ ti.xcom_pull(task_ids='get_your_pet_name') }}",
        ],
    )

    consume_treats = ConsumeFromTopicOperator(
        task_id="consume_treats",
        kafka_config_id="kafka_default",
        topics=[KAFKA_TOPIC],
        apply_function="produce_consume_treats.consume_function",
        apply_function_kwargs={
            "owner_name": "{{ ti.xcom_pull(task_ids='get_pet_owner_name') }}"
        },
        poll_timeout=10,
        max_messages=NUMBER_OF_TREATS,
    )

    (
        [
            get_number_of_treats(NUMBER_OF_TREATS),
            get_your_pet_name(YOUR_PET_NAME),
            get_pet_owner_name(YOUR_NAME),
        ]
        >> produce_treats
        >> consume_treats
    )

produce_consume_treats() 