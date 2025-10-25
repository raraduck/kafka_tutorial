# 카프카 다중 브로커 환경 실습 가이드

이 가이드는 Kafka 다중 브로커 환경에서 파티션, 복제, 장애 대응, 고가용성 등을 실습하는 단계별 안내서입니다.

## 목차

1. [환경 설정](#1-환경-설정)
2. [다중 파티션 및 복제 팩터 설정](#2-다중-파티션-및-복제-팩터-설정)
3. [브로커 장애 시나리오 테스트](#3-브로커-장애-시나리오-테스트)
4. [고가용성 검증](#4-고가용성-검증)
5. [파티셔닝 전략](#5-파티셔닝-전략)
6. [리밸런싱 관찰](#6-리밸런싱-관찰)
7. [문제 해결](#7-문제-해결)

---

## 1. 환경 설정

### 1.1 Docker 환경 시작

```bash
# Docker Compose를 이용하여 Kafka 다중 브로커 환경 시작
cd /Users/allene.ha/Desktop/kafka/kafka-docker-envs/multi-broker
docker-compose up -d
```

### 1.2 컨테이너 상태 확인

```bash
# 모든 컨테이너가 실행 중인지 확인
docker-compose ps

# 각 브로커의 로그 확인 (문제 해결 시)
docker logs kafka1
docker logs kafka2
docker logs kafka3
```

### 1.3 Kafka UI 접속

브라우저에서 `http://localhost:8080`에 접속하여 Kafka UI를 통해 브로커, 토픽, 파티션 상태를 시각적으로 확인할 수 있습니다.

---

## 2. 다중 파티션 및 복제 팩터 설정

### 2.1 기본 개념

- **파티션(Partition)**: 토픽의 데이터를 여러 브로커에 분산 저장하기 위한 단위
- **복제 팩터(Replication Factor)**: 각 파티션의 복제본 수 (장애 대응을 위해 중요)
- **리더(Leader)**: 각 파티션의 읽기/쓰기를 담당하는 브로커
- **팔로워(Follower)**: 리더의 데이터를 복제하는 브로커

### 2.2 토픽 생성

다양한 파티션 수와 복제 팩터를 설정하여 토픽을 생성해 봅니다.

```bash
# 3개 파티션, 3개 복제 팩터를 가진 토픽 생성
./create_topic.sh orders 3 3

# 5개 파티션, 2개 복제 팩터를 가진 또 다른 토픽 생성
./create_topic.sh events 5 2

# 1개 파티션, 3개 복제 팩터를 가진 토픽 생성
./create_topic.sh logs 1 3
```

### 2.3 토픽 정보 확인

```bash
# 모든 토픽 목록 확인
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --list

# 특정 토픽의 파티션 및 복제 상태 확인
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic orders
```

출력 예시:
```
Topic: orders    Partitions: 3    Replication Factor: 3    Configs: 
    Topic: orders    Partition: 0    Leader: 1    Replicas: 1,2,3    Isr: 1,2,3
    Topic: orders    Partition: 1    Leader: 2    Replicas: 2,3,1    Isr: 2,3,1
    Topic: orders    Partition: 2    Leader: 3    Replicas: 3,1,2    Isr: 3,1,2
```

각 항목의 의미:
- **Leader**: 해당 파티션의 읽기/쓰기를 담당하는 브로커 ID
- **Replicas**: 해당 파티션의 복제본을 가지고 있는 브로커 ID 목록
- **Isr** (In-Sync Replicas): 리더와 동기화된 상태인 복제본 목록

---

## 3. 브로커 장애 시나리오 테스트

### 3.1 브로커 장애 테스트 스크립트 실행

```bash
# 브로커 장애 테스트 스크립트 실행
./test_broker_failure.sh
```

메뉴가 나타나면 테스트할 시나리오를 선택합니다:
1. 단일 브로커 장애 테스트 (한 개의 브로커 중단)
2. 다중 브로커 장애 테스트 (두 개의 브로커 중단)
3. 리더 브로커 장애 테스트 (파티션 리더를 포함한 브로커 중단)
4. 모든 시나리오 실행

### 3.2 수동으로 브로커 장애 테스트

특정 브로커를 직접 중단하고 복구하는 방법:

```bash
# kafka1 브로커 중단
docker stop kafka1

# 토픽 상태 확인 (리더 변경 확인)
docker exec -it kafka2 kafka-topics --bootstrap-server kafka2:29093 --describe --topic orders

# kafka1 브로커 재시작
docker start kafka1

# 복구 후 토픽 상태 확인
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic orders
```

### 3.3 장애 상황에서의 메시지 전송/수신 테스트

브로커 중단 상태에서도 메시지 전송과 수신이 가능한지 테스트합니다.

```bash
# 터미널 1에서 Producer 실행
python producer_example.py

# 터미널 2에서 Consumer 실행
python consumer_example.py

# 터미널 3에서 브로커 중단
docker stop kafka3
```

메시지가 계속 전송되고 수신되는지 관찰합니다. 리더 변경이 일어나도 메시지 유실 없이 서비스가 계속 동작하는지 확인합니다.

---

## 4. 고가용성 검증

### 4.1 ISR(In-Sync Replicas) 확인

ISR은 리더와 동기화된 상태인 복제본 목록으로, 고가용성의 핵심 지표입니다.

```bash
# orders 토픽의 ISR 상태 확인
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic orders | grep "Isr:"
```

### 4.2 장애 상황에서 ISR 변화 관찰

1. 초기 ISR 상태를 확인합니다.
2. 브로커를 중단합니다:
   ```bash
   docker stop kafka2
   ```
3. ISR 변화를 관찰합니다:
   ```bash
   docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic orders | grep "Isr:"
   ```
4. 브로커를 복구하고 ISR이 다시 복원되는지 확인합니다:
   ```bash
   docker start kafka2
   # 잠시 기다린 후
   docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic orders | grep "Isr:"
   ```

### 4.3 Unclean Leader Election 테스트 (선택 사항)

다수 브로커 장애 상황에서 ISR 목록에 없는 브로커가 리더가 되는 상황을 테스트합니다.

```bash
# 복제 팩터 3, 파티션 1, min.insync.replicas=2로 설정된 새 토픽 생성
docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --create \
  --topic critical-data --partitions 1 --replication-factor 3 \
  --config min.insync.replicas=2

# 두 브로커 중단 (ISR 조건 불충족)
docker stop kafka2 kafka3

# 이 상태에서 메시지 전송 시도 (acks=all 설정 시 예외 발생 예상)
# 브로커 1만 남은 상태에서 critical-data 토픽에 메시지 전송 시도
docker exec -it kafka1 kafka-console-producer --bootstrap-server kafka1:29092 \
  --topic critical-data \
  --producer-property acks=all
```

---

## 5. 파티셔닝 전략

### 5.1 파티셔닝 기본 개념

Kafka는 다음과 같은 파티셔닝 전략을 제공합니다:
- **Round-Robin**: 키가 없는 경우 순차적으로 파티션 할당
- **Key-Based**: 키의 해시값에 따라 특정 파티션에 할당 (같은 키는 같은 파티션)
- **Custom**: 사용자 정의 파티셔너를 구현하여 파티션 지정

### 5.2 키 기반 파티셔닝 테스트

제공된 `producer_example.py` 코드는 사용자 ID를 키로 사용하여 같은 사용자의 주문은 항상 같은 파티션으로 전송되도록 구현되어 있습니다.

```bash
# Producer 실행 (user_id를 키로 사용)
python producer_example.py
```

### 5.3 파티션 할당 확인

여러 메시지를 전송한 후, 콘솔 출력에서 같은 user_id가 동일한 파티션으로 할당되는지 확인합니다. 또는 각 파티션의 메시지를 직접 확인할 수 있습니다:

```bash
# 파티션 0의 메시지만 확인
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 \
  --topic orders --partition 0 --from-beginning

# 파티션 1의 메시지만 확인
docker exec -it kafka1 kafka-console-consumer --bootstrap-server kafka1:29092 \
  --topic orders --partition 1 --from-beginning
```

---

## 6. 리밸런싱 관찰

### 6.1 리밸런싱 개념

리밸런싱은 컨슈머 그룹 내에서 파티션 할당이 변경되는 과정입니다. 다음과 같은 경우에 발생합니다:
- 컨슈머 추가/제거
- 브로커 추가/제거
- 토픽 파티션 수 변경

### 6.2 컨슈머 그룹 생성

```bash
# 컨슈머 그룹 생성 및 토픽 구독
./create_topic.sh orders 3 3 with-group
```

### 6.3 멀티스레드 컨슈머 실행

제공된 `consumer_example.py`는 멀티스레드 컨슈머를 구현하여 여러 파티션을 병렬로 처리합니다.

```bash
# 3개 스레드로 컨슈머 시작
python consumer_example.py --threads 3

# 다른 터미널에서 현재 컨슈머 그룹 상태 확인
docker exec -it kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 \
  --describe --group order_processing_group
```

### 6.4 리밸런싱 유발

컨슈머를 추가하거나 제거하여 리밸런싱을 유발합니다:

```bash
# 새 터미널에서 추가 컨슈머 실행
python consumer_example.py --threads 2

# 또는 실행 중인 컨슈머 중지 (Ctrl+C)
```

리밸런싱 로그를 관찰하고 컨슈머 그룹 상태 변화를 확인합니다:

```bash
# 리밸런싱 후 컨슈머 그룹 상태 확인
docker exec -it kafka1 kafka-consumer-groups --bootstrap-server kafka1:29092 \
  --describe --group order_processing_group
```

---

## 7. 문제 해결

### 7.1 일반적인 문제 및 해결 방법

| 문제 | 해결 방법 |
|------|-----------|
| 브로커 연결 실패 | 네트워크 설정 확인, 포트 충돌 확인, 컨테이너 재시작 |
| ISR이 복구되지 않음 | 브로커 로그 확인, 디스크 공간 확인, 네트워크 지연 확인 |
| 메시지 전송 실패 | Producer 로그 확인, 브로커 상태 확인, 토픽 설정 확인 |
| 컨슈머 그룹 리밸런싱 반복 | Consumer 타임아웃 설정 확인, 과부하 확인 |

### 7.2 로그 확인 방법

```bash
# 브로커 로그 확인
docker logs --tail 100 kafka1

# Zookeeper 로그 확인 (브로커 상태 관련 문제)
docker logs --tail 100 zookeeper
```

### 7.3 환경 초기화

문제가 지속될 경우 환경을 초기화하고 다시 시작합니다:

```bash
# 컨테이너 중지 및 삭제
docker-compose down

# 볼륨 삭제 (선택 사항)
docker volume prune

# 다시 시작
docker-compose up -d
```

---

## 결론

이 가이드를 통해 Kafka 다중 브로커 환경에서 다음 개념들을 실습하고 이해할 수 있습니다:
- 다중 파티션 및 복제 설정을 통한 확장성과 고가용성 구성
- 브로커 장애 시 시스템의 동작 방식과 복구 프로세스
- 파티셔닝 전략을 통한 메시지 분산과 순서 보장
- 컨슈머 그룹과 리밸런싱 동작 원리

이러한 개념들은 실제 프로덕션 환경에서 안정적인 Kafka 클러스터를 운영하는 데 필수적인 요소들입니다. 