# Kafka tutorial

# 카프카 단일 브로커 환경 실습 가이드

이 가이드는 Docker Compose를 사용하여 구성된 단일 브로커 Kafka 환경에서의 기본적인 실습 과정을 안내합니다.

## 사전 준비

1. Docker 및 Docker Compose가 설치되어 있어야 합니다.
   ```bash
   # 설치 확인
   docker --version
   docker-compose --version
   ```

2. Python 3.x 및 kafka-python 라이브러리가 설치되어 있어야 합니다.
   ```bash
   # Python 버전 확인
   python --version
   
   # kafka-python 라이브러리 설치
   pip install kafka-python
   ```
   
   > **주의**: kafka-python 라이브러리가 설치되어 있지 않으면 Python 예제 실행 시 `ModuleNotFoundError: No module named 'kafka'` 오류가 발생합니다.

3. 실행 권한 부여 (필요한 경우)
   ```bash
   chmod +x create_topic.sh
   ```

## 환경 시작하기

먼저 Docker Compose를 사용하여 Kafka 및 Zookeeper 컨테이너를 시작합니다:

```bash
docker-compose up -d
```

컨테이너가 정상적으로 시작되었는지 확인합니다:

```bash
docker-compose ps
```

모든 컨테이너가 `Up` 상태인지 확인하세요.

> **팁**: 카프카 컨테이너가 완전히 시작되기까지 약 30초 정도 기다려야 합니다. 컨테이너 상태가 `Up (health: starting)`인 경우 잠시 기다렸다가 다음 단계를 진행하세요.

## Step 1: 토픽 생성

Kafka는 토픽을 통해 메시지를 분류합니다. 먼저 실습에 사용할 토픽을 생성해보겠습니다.

### 1.1 기본 토픽 생성

제공된 스크립트를 사용하여 토픽을 생성합니다:

```bash
./create_topic.sh my_topic 1 1
```

이 명령은 `my_topic`이라는 이름의 토픽을 생성하며, 파티션 수는 1, 복제 팩터는 1로 설정합니다.

> **토픽 생성 매개변수 설명**:
> - 첫 번째 매개변수: 토픽 이름
> - 두 번째 매개변수: 파티션 수 (메시지 병렬 처리 단위)
> - 세 번째 매개변수: 복제 팩터 (데이터 복제본 수, 고가용성 관련)

### 1.2 토픽 목록 확인

생성된 토픽 목록을 확인합니다:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### 1.3 토픽 상세 정보 확인

특정 토픽의 상세 정보를 확인합니다:

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic my_topic
```

## Step 2: Hello World 예제

가장 간단한 방식으로 Kafka를 사용하는 예제입니다. Kafka CLI 도구를 사용하여 메시지를 전송하고 소비합니다.

### 2.1 콘솔 프로듀서로 메시지 보내기

새 터미널을 열고 다음 명령어를 실행합니다:

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:9092 --topic my_topic
```

프롬프트가 나타나면 메시지를 입력하고 Enter 키를 누릅니다:
```
>Hello Kafka!
>This is a test message
>Welcome to Kafka
```

`Ctrl+C`를 눌러 프로듀서를 종료합니다.

> **대안적 방법**: 대화형 콘솔을 사용하지 않고 바로 메시지를 전송하려면 다음과 같이 echo 명령어와 파이프라인을 사용할 수 있습니다:
> ```bash
> echo "Hello Kafka!" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic my_topic
> ```
> 이 방식은 스크립트에서 메시지를 자동화할 때 유용합니다.

### 2.2 콘솔 컨슈머로 메시지 읽기

새 터미널을 열고 다음 명령어를 실행합니다:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic my_topic --from-beginning
```

이전에 전송한 메시지들이 출력되는 것을 확인할 수 있습니다.
```
Hello Kafka!
This is a test message
Welcome to Kafka
```

`Ctrl+C`를 눌러 컨슈머를 종료합니다.

> **팁**: 컨슈머가 무한정 대기하지 않도록 타임아웃을 설정하고, 대화형 인터페이스를 방지하려면 다음과 같이 명령어를 실행할 수 있습니다:
> ```bash
> docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic my_topic --from-beginning --timeout-ms 10000 | cat
> ```
> 이 명령은 10초 동안만 메시지를 수신한 후 종료됩니다.

## Step 3: 기본 Producer 예제

Python 코드를 사용하여 조금 더 복잡한 프로듀서 예제를 실행합니다.

### 3.1 producer_example.py 실행

제공된 `producer_example.py` 스크립트를 실행합니다:

```bash
python producer_example.py
```

이 스크립트는 5초마다 임의의 사용자 활동 메시지를 생성하여 `user_activity` 토픽으로 전송합니다. 각 메시지는 JSON 형식으로 직렬화됩니다.

> **참고**: 스크립트를 실행하기 전에 kafka-python 라이브러리가 설치되어 있는지 확인하세요. 설치되어 있지 않으면 `pip install kafka-python` 명령으로 설치할 수 있습니다.

### 3.2 프로듀서 코드 이해하기

`producer_example.py` 파일을 열어 코드를 살펴봅니다:

```python
# 주요 구성 요소:
# 1. KafkaProducer 초기화: 브로커 주소, 직렬화 방식, acks 설정 등
# 2. 메시지 생성 함수: 임의의 메시지 데이터 생성
# 3. 비동기 콜백 함수: 메시지 전송 성공/실패 시 실행
# 4. 메시지 전송 루프: 반복적으로 메시지 전송
```

주요 설정 옵션들:
- `bootstrap_servers`: Kafka 브로커 주소
- `value_serializer`: 메시지 값을 직렬화하는 함수
- `acks`: 메시지 수신 확인 레벨 (all, 1, 0)

> **acks 옵션 설명**:
> - `acks=0`: 프로듀서는 서버의 응답을 기다리지 않음 (최고 성능, 메시지 손실 가능성 높음)
> - `acks=1`: 리더 브로커의 응답만 기다림 (적절한 성능과 신뢰성 균형)
> - `acks=all`: 모든 복제본의 응답을 기다림 (최고 신뢰성, 상대적으로 낮은 성능)

### 3.3 프로듀서 실행 결과 확인

메시지가 성공적으로 전송되면 다음과 같은 출력을 볼 수 있습니다:
```
메시지 전송: {'user_id': 123, 'action': 'login', 'timestamp': 1646092800.0}
메시지 전송 성공 - 토픽: user_activity, 파티션: 0, 오프셋: 0
```

## Step 4: 기본 Consumer 예제

Python 코드를 사용하여 컨슈머 예제를 실행합니다.

### 4.1 새 터미널에서 consumer_example.py 실행

새 터미널을 열고 다음 명령어를 실행합니다:

```bash
python consumer_example.py
```

이 스크립트는 `user_activity` 토픽을 구독하고 수신된 메시지를 처리합니다.

### 4.2 컨슈머 코드 이해하기

`consumer_example.py` 파일을 열어 코드를 살펴봅니다:

```python
# 주요 구성 요소:
# 1. KafkaConsumer 초기화: 브로커 주소, 토픽, 그룹 ID, 역직렬화 방식 등
# 2. 메시지 처리 함수: 수신된 메시지를 처리하는 로직
# 3. 메시지 소비 루프: 지속적으로 메시지를 소비
```

주요 설정 옵션들:
- `bootstrap_servers`: Kafka 브로커 주소
- `group_id`: 컨슈머 그룹 ID
- `auto_offset_reset`: 초기 오프셋 위치 ('earliest', 'latest')
- `enable_auto_commit`: 자동 오프셋 커밋 여부
- `value_deserializer`: 수신된 메시지 값을 역직렬화하는 함수

> **auto_offset_reset 옵션 설명**:
> - `earliest`: 가장 오래된 메시지부터 소비 (모든 기록된 메시지 포함)
> - `latest`: 가장 최근 메시지부터 소비 (새로 들어오는 메시지만 포함)

### 4.3 컨슈머 그룹 확인

컨슈머 그룹 상태를 확인합니다:

```bash
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group user_activity_group
```

> **참고**: 컨슈머가 실행 중이지 않으면 "Consumer group has no active members" 메시지가 표시될 수 있습니다. 이는 정상적인 상황입니다.

### 4.4 컨슈머와 프로듀서 통합 테스트

1. 프로듀서(`producer_example.py`)를 한 터미널에서 실행합니다.
2. 컨슈머(`consumer_example.py`)를 다른 터미널에서 실행합니다.
3. 프로듀서가 보낸 메시지가 컨슈머에서 실시간으로 처리되는 것을 확인합니다.

컨슈머 출력 예시:
```
======= 새 메시지 수신 (파티션: 0, 오프셋: 1) =======
🟢 사용자 123가 로그인했습니다.

======= 새 메시지 수신 (파티션: 0, 오프셋: 2) =======
💰 사용자 456가 500원 결제했습니다.
```

## 심화 실습 (선택 사항)

### 1. 다양한 메시지 전송 방식 테스트

`producer_example.py`를 수정하여 다양한 전송 방식을 테스트해볼 수 있습니다:

- 동기 전송: `future.get()` 사용
- 비동기 전송: 기본 구현 방식
- 키 기반 파티셔닝: `key` 파라미터 추가

### 2. 오프셋 관리

`consumer_example.py`를 수정하여 수동 오프셋 관리를 테스트해볼 수 있습니다:

- `enable_auto_commit=False` 설정
- 메시지 처리 후 `consumer.commit()` 호출
- 특정 오프셋부터 읽기: `consumer.seek()` 사용

### 3. 여러 컨슈머 실행

동일한 그룹 ID를 가진 여러 컨슈머 인스턴스를 실행하여 파티션 할당 동작을 관찰할 수 있습니다. 그러나 이 예제에서는 파티션이 1개이므로 한 번에 하나의 컨슈머만 활성화됩니다.

## 환경 정리

실습을 마치면 다음 명령어로 환경을 정리합니다:

```bash
docker-compose down
```

볼륨도 함께 삭제하려면:

```bash
docker-compose down -v
```

> **주의**: `-v` 옵션을 사용하면 모든 데이터와 설정이 영구적으로 삭제됩니다. 필요한 데이터가 있다면 먼저 백업하세요.

## 문제 해결

### 1. 컨테이너 로그 확인

```bash
docker-compose logs kafka
docker-compose logs zookeeper
```

### 2. 네트워크 확인

```bash
docker network ls
docker network inspect kafka-docker-envs_default
```

### 3. 토픽 삭제 (필요한 경우)

```bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic my_topic
```

### 4. 자주 발생하는 문제와 해결책

#### 카프카 연결 오류
- **증상**: `Connection refused` 또는 `Connection timed out` 오류
- **해결책**: 
  - 카프카 컨테이너가 실행 중인지 확인합니다: `docker-compose ps`
  - 카프카 컨테이너가 완전히 시작될 때까지 기다립니다 (약 30초)
  - 호스트 IP 주소가 올바른지 확인합니다

#### Python 모듈 오류
- **증상**: `ModuleNotFoundError: No module named 'kafka'`
- **해결책**: `pip install kafka-python` 명령으로 필요한 라이브러리를 설치합니다

#### 컨슈머가 메시지를 받지 못하는 경우
- **증상**: 프로듀서는 메시지를 전송하지만 컨슈머에서 수신되지 않음
- **해결책**:
  - 토픽 이름이 일치하는지 확인합니다
  - `auto_offset_reset` 설정을 `earliest`로 변경하여 모든 메시지를 받도록 합니다
  - 컨슈머 그룹 ID가 이전에 사용된 경우, 새로운 ID를 사용하거나 오프셋을 리셋합니다

## 추가 학습 자료

- [Apache Kafka 공식 문서](https://kafka.apache.org/documentation/)
- [Confluent Kafka 튜토리얼](https://developer.confluent.io/tutorials/)
- [kafka-python 라이브러리 문서](https://kafka-python.readthedocs.io/) 