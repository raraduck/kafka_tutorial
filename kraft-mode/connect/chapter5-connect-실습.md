
# PostgreSQL-Kafka Connect 파이프라인 실습

**PostgreSQL → Kafka Connect → Kafka → Kafka Connect → PostgreSQL** 파이프라인을 구축하는 실습가이드 입니다.

## 목차

1. [사전 준비](#1-사전-준비)
2. [Docker Compose 환경 구성](#2-docker-compose-환경-구성)
3. [PostgreSQL 설정](#3-postgresql-설정)
4. [Kafka Connect 설정](#4-kafka-connect-설정)
5. [Source Connector 구성](#5-source-connector-구성)
6. [Sink Connector 구성](#6-sink-connector-구성)
7. [파이프라인 검증](#7-파이프라인-검증)
8. [실시간 데이터 변경 테스트](#8-실시간-데이터-변경-테스트)
9. [정리 및 마무리](#9-정리-및-마무리)

## 1. 사전 준비

### 필요한 소프트웨어
- Docker 및 Docker Compose
- 터미널 또는 명령 프롬프트
- 텍스트 에디터 (VS Code 등)
- 웹 브라우저 (Confluent Control Center 접속용)

Docker와 Docker Compose가 설치되어 있는지 확인하세요.
```bash
# Docker 버전 확인
docker --version

# Docker Compose 버전 확인
docker-compose --version

# Docker 서비스 실행 상태 확인
docker info
```

## 2. Docker Compose 환경 구성

### Docker Compose 환경 준비

이 실습에서는 Confluent Platform의 cp-all-in-one-kraft 환경을 사용합니다. 이 환경은 Kafka KRaft 모드를 사용하여 Zookeeper 없이 Kafka를 실행합니다.

```bash
# 작업 디렉토리 생성 및 이동
mkdir -p ~/kafka-postgres
cd ~/kafka-postgres

# Confluent Platform GitHub 저장소 클론
git clone https://github.com/confluentinc/cp-all-in-one.git
cd cp-all-in-one/cp-all-in-one-kraft
```

### docker-compose.yml 파일에 PostgreSQL 추가

cp-all-in-one-kraft의 docker-compose.yml 파일에 PostgreSQL 서비스를 추가합니다.

```bash
# 텍스트 에디터로 docker-compose.yml 파일 열기
vi docker-compose.yml  # 또는 원하는 에디터를 사용하세요
```

파일 맨 아래에 다음 내용을 추가합니다:

```yaml
  postgres:
    image: postgres:latest
    hostname: postgres
    container_name: postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: kafka_demo
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres-data:
```

### KSQLdb 관련 서비스 비활성화 (필요시)

Docker Compose 파일에서 control-center가 ksqldb-server에 의존성이 있다면, 다음과 같이 수정합니다:

```yaml
  control-center:
    # ... 기존 설정 ...
    depends_on:
      - broker
      - schema-registry
      - connect
      # - ksqldb-server  # 이 라인 제거 또는 주석 처리
```

### Docker Compose 환경 시작

모든 서비스를 시작합니다:

```bash
# Docker Compose 실행 (백그라운드 모드)
docker-compose up -d

# 컨테이너 실행 상태 확인
docker-compose ps

# 모든 서비스가 정상적으로 실행되었는지 확인
docker ps
```

## 3. PostgreSQL 설정

### PostgreSQL 테이블 생성 및 샘플 데이터 추가

PostgreSQL에 필요한 테이블을 생성하고 샘플 데이터를 삽입합니다.

```bash
# source_table 생성 (원본 데이터 테이블)
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"CREATE TABLE source_table (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);\""

# target_table 생성 (복제될 대상 테이블)
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"CREATE TABLE target_table (id INT PRIMARY KEY, name VARCHAR(100), email VARCHAR(100), created_at TIMESTAMP, processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);\""

# 샘플 데이터 삽입
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"INSERT INTO source_table (name, email) VALUES ('사용자1', 'user1@example.com'), ('사용자2', 'user2@example.com'), ('사용자3', 'user3@example.com');\""

# 데이터 확인
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"SELECT * FROM source_table;\""
```

**학습 포인트**:
- `source_table`에는 SERIAL 타입의 id 필드가 있어 자동으로 증가합니다.
- `target_table`에는 processed_at 필드가 있어 데이터가 처리된 시간을 기록합니다.
- `created_at` 필드는 source_table에서는 자동으로 설정되지만, target_table에서는 소스의 값을 그대로 복사합니다.

## 4. Kafka Connect 설정

### PostgreSQL JDBC 드라이버 설치

Kafka Connect가 PostgreSQL과 통신하기 위한 JDBC 드라이버를 설치합니다.

```bash
# PostgreSQL JDBC 드라이버 다운로드
docker exec connect curl -L -o /tmp/postgresql-42.5.1.jar https://jdbc.postgresql.org/download/postgresql-42.5.1.jar

# JDBC 드라이버용 디렉토리 생성
docker exec connect mkdir -p /usr/share/confluent-hub-components/jdbc-connector

# 다운로드한 드라이버를 디렉토리로 복사
docker exec connect cp /tmp/postgresql-42.5.1.jar /usr/share/confluent-hub-components/jdbc-connector/

# JDBC 커넥터 설치
docker exec connect confluent-hub install --no-prompt --component-dir /usr/share/confluent-hub-components confluentinc/kafka-connect-jdbc:10.8.3

# Connect 컨테이너 재시작
docker restart connect

# Connect 서비스가 정상적으로 실행되었는지 확인
docker ps | grep connect
```

## 5. Source Connector 구성

PostgreSQL에서 Kafka로 데이터를 전송하는 Source Connector를 구성합니다.

### Source Connector 구성 파일 생성

먼저 `postgres-source.json` 파일을 생성합니다:

```bash
cat > postgres-source.json << 'EOF'
{
  "name": "postgres-source-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/kafka_demo",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "topic.prefix": "postgres-source-",
    "table.whitelist": "source_table",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "tasks.max": "1"
  }
}
EOF
```

### Source Connector 등록

```bash
# Source Connector 등록
curl -X POST -H "Content-Type: application/json" --data @postgres-source.json http://localhost:8083/connectors

# 등록 확인
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq
```

**학습 포인트**:
- `connector.class`: JDBC Source Connector 클래스를 사용합니다.
- `connection.url`: PostgreSQL 연결 URL을 설정합니다.
- `topic.prefix`: 생성될 Kafka 토픽 이름의 접두사입니다.
- `mode`: 증분 모드로 설정하여 새로운 레코드나 변경된 레코드만 가져옵니다.
- `incrementing.column.name`: 증분 값을 확인할 컬럼명입니다.

## 6. Sink Connector 구성

Kafka에서 PostgreSQL로 데이터를 다시 전송하는 Sink Connector를 구성합니다.

### Sink Connector 구성 파일 생성

`postgres-sink.json` 파일을 다음 내용으로 생성합니다:

```bash
cat > postgres-sink.json << 'EOF'
{
  "name": "postgres-sink-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "connection.url": "jdbc:postgresql://postgres:5432/kafka_demo",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "topics": "postgres-source-source_table",
    "table.name.format": "target_table",
    "auto.create": "false",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "id",
    "tasks.max": "1"
  }
}
EOF
```

### Sink Connector 등록

```bash
# Sink Connector 등록
curl -X POST -H "Content-Type: application/json" --data @postgres-sink.json http://localhost:8083/connectors

# 등록 확인
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq
```

**학습 포인트**:
- `topics`: Source Connector에서 생성한 토픽을 지정합니다.
- `table.name.format`: 데이터를 저장할 테이블 이름을 지정합니다.
- `auto.create`: 테이블이 없을 경우 자동 생성할지 여부를 설정합니다.
- `insert.mode`: upsert 모드를 사용하여 기존 데이터가 있으면 업데이트하고, 없으면 삽입합니다.
- `pk.mode` & `pk.fields`: 프라이머리 키로 사용할 필드를 지정합니다.

## 7. 파이프라인 검증

### Connector 상태 확인

```bash
# 등록된 커넥터 목록 확인
curl -s http://localhost:8083/connectors | jq

# Source Connector 상태 확인
curl -s http://localhost:8083/connectors/postgres-source-connector/status | jq

# Sink Connector 상태 확인
curl -s http://localhost:8083/connectors/postgres-sink-connector/status | jq
```

### Kafka 토픽 메시지 확인

```bash
# 토픽 목록 확인
docker exec broker kafka-topics --bootstrap-server broker:29092 --list | grep postgres

# 메시지 내용 확인
docker exec broker kafka-console-consumer --bootstrap-server broker:29092 --topic postgres-source-source_table --from-beginning --max-messages 5
```

### 대상 테이블 데이터 확인

```bash
# PostgreSQL에서 대상 테이블 확인
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"SELECT * FROM target_table;\""
```

## 8. 실시간 데이터 변경 테스트

### 소스 테이블에 새 데이터 추가

```bash
# PostgreSQL 컨테이너에 접속하여 소스 테이블에 데이터 추가
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"INSERT INTO source_table (name, email) VALUES ('새사용자', 'newuser@example.com');\""

# 소스 테이블 확인
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"SELECT * FROM source_table ORDER BY id DESC LIMIT 1;\""
```

### 데이터 복제 확인

```bash
# 데이터 복제까지 약간의 지연이 있을 수 있으므로 잠시 기다림
sleep 5

# 대상 테이블 확인
docker exec -it postgres bash -c "psql -U postgres -d kafka_demo -c \"SELECT * FROM target_table ORDER BY id DESC LIMIT 1;\""
```

## 9. 정리 및 마무리

### 커넥터 삭제 (실습 완료 후)

```bash
# Source Connector 삭제
curl -X DELETE http://localhost:8083/connectors/postgres-source-connector

# Sink Connector 삭제
curl -X DELETE http://localhost:8083/connectors/postgres-sink-connector
```

### Docker Compose 환경 정지 (실습 완료 후)

```bash
# Docker Compose 환경 정지 및 컨테이너 제거
docker-compose down

# 볼륨까지 모두 제거하려면
docker-compose down -v
```

## Confluent Control Center 활용

Confluent Platform은 웹 기반 관리 도구인 Control Center를 제공합니다. 이를 통해 Kafka와 Connect를 GUI로 관리할 수 있습니다.

```bash
# Control Center 접속 URL 확인
echo "Confluent Control Center: http://localhost:9021"
```

Control Center에서 다음 작업을 수행할 수 있습니다:
1. Kafka Connect 클러스터 관리
2. 커넥터 생성, 수정, 삭제
3. 토픽 관리 및 메시지 내용 확인
4. 시스템 모니터링

## 학습 요약

이 실습을 통해 다음 내용을 학습했습니다:

1. **Kafka Connect의 기본 개념과 구성 방법**
   - Source Connector와 Sink Connector의 역할과 차이점
   - JDBC Connector를 사용한 데이터베이스 연동 방법

2. **실시간 데이터 파이프라인 구축**
   - PostgreSQL → Kafka → PostgreSQL로 이어지는 데이터 흐름 구성
   - 증분 모드를 통한 변경 데이터 추적 방법

3. **Confluent Platform 활용**
   - Kafka 브로커, Schema Registry, Connect 등의 구성 요소 활용
   - Control Center를 통한 시스템 관리 방법

## 심화 학습 방향

- 다양한 Connector 유형 (Elasticsearch, MongoDB, S3 등) 실습
- 변환(Transforms)을 적용한 데이터 변환 파이프라인 구성
- 고가용성 Connect 클러스터 구성
- 보안 설정 적용 (SSL, SASL 등)

## 참고 자료

- [Confluent JDBC Connector 공식 문서](https://docs.confluent.io/kafka-connectors/jdbc/current/index.html)
- [Kafka Connect 공식 문서](https://kafka.apache.org/documentation/#connect)
- [Confluent Platform 문서](https://docs.confluent.io/platform/current/overview.html)
- [PostgreSQL 공식 문서](https://www.postgresql.org/docs/) 