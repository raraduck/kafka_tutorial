# Kafka + Spark Structured Streaming 실습

Kafka와 Spark Structured Streaming을 사용한 실시간 데이터 처리 파이프라인을 구축하는 실습을 단계별로 설명합니다.

## 시나리오 요약

**기본 스트리밍 파이프라인 구축 (`basic_kafka_streaming.py`)**
  * Kafka → Spark Structured Streaming → Kafka/Console/File로 흐르는 완전한 실시간 파이프라인

**비즈니스 로직을 스트리밍 애플리케이션에 통합**

<div style="text-align: center;">
<div class="mermaid">
%%{ init: { "theme": "neutral" } }%%
flowchart TD
    A[Data Generator] --> B[Kafka Topic: stream_data_topic]:::kafka
    B --> C["Anomaly Detection<br>(z-score)"]:::spark
    B --> D["Windowed Aggregation <br>(Tumbling/Sliding/Hourly)"]:::spark
    C --> E[Kafka: anomaly_alerts_topic]:::kafka
    D --> F[Kafka: windowed_results_topic]:::kafka

    classDef kafka fill:#bfdbfe,stroke:#1d4ed8,color:#1e3a8a,stroke-width:2px;
    classDef spark fill:#fde68a,stroke:#92400e,color:#1f2937,stroke-width:2px;

</div>
</div>

  * 윈도우 집계 스트리밍 애플리케이션 실습 (`windowed_aggregation.py`)
  * 이상 감지 스트리밍 애플리케이션 실습 (`anomaly_detection_streaming.py`)


## 사전 준비

### 환경 구성
1. Docker와 Docker Compose가 설치되어 있어야 합니다.
2. `kafka-spark-airflow` 디렉토리가 있어야 합니다.


## 시나리오 1: 기본 스트리밍 파이프라인 구축

### 1단계: 환경 설정

먼저 Docker Compose를 사용하여 필요한 서비스를 시작합니다:

```bash
cd kafka-spark-airflow
docker-compose up -d
```

모든 컨테이너가 정상적으로 실행되고 있는지 확인합니다:

```bash
docker ps
```

출력에서 `kafka`, `zookeeper`, `spark-master`, `spark-worker` 등의 컨테이너가 실행 중임을 확인해야 합니다.

### 2단계: Kafka 토픽 설정

데이터 소스용 토픽을 생성합니다:

```bash
docker exec -it kafka kafka-topics --create --topic stream_data_topic --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
```

처리 결과 저장용 토픽을 생성합니다:

```bash
docker exec -it kafka kafka-topics --create --topic processed_data_topic --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
```

토픽이 성공적으로 생성되었는지 확인합니다:

```bash
docker exec -it kafka kafka-topics --list --bootstrap-server kafka:29092
```

### 3단계: 테스트 데이터 생성

데이터 생성기(`data_generator.py`)의 핵심 부분은 다음과 같습니다:

```python
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
```

데이터 생성기를 실행하여 테스트 데이터를 생성합니다:
```bash
# 데이터 생성기 실행 전 필요한 라이브러리 설치
docker exec -it spark-master pip install confluent-kafka

# 데이터 생성기 실행
docker exec -it spark-master python /opt/spark-apps/data_generator.py
```

생성된 데이터를 확인합니다:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic stream_data_topic --from-beginning --max-messages 5
```

메시지 예시:
```json
{"timestamp": "2025-04-20T13:40:45.765298", "value": 62.9, "device_id": "device_003", "status": "Danger"}
{"timestamp": "2025-04-20T13:40:45.765306", "value": 59.82, "device_id": "device_004", "status": "Normal"}
```

### 4단계: 기본 스트리밍 애플리케이션 실행

출력 디렉토리를 생성합니다:

```bash
docker exec -it spark-master mkdir -p /tmp/output/streaming_data
docker exec -it spark-master mkdir -p /tmp/checkpoint/file-output
docker exec -it spark-master mkdir -p /tmp/checkpoint/kafka-to-kafka
```

`basic_kafka_streaming.py` 스크립트의 주요 코드는 다음과 같습니다:

```python
def read_from_kafka(spark, topic):
    """Kafka에서 스트리밍 데이터 읽기"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def parse_data(kafka_df):
    """JSON 메시지 파싱"""
    # 예시 스키마 정의 (실제 데이터에 맞게 수정 필요)
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("device_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # JSON 파싱 및 타임스탬프 추가
    return kafka_df \
        .selectExpr("CAST(key AS STRING) as message_key", "CAST(value AS STRING) as message_value") \
        .select(
            col("message_key"),
            from_json(col("message_value"), schema).alias("data")
        ) \
        .select("message_key", "data.*") \
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withColumn("processing_time", current_timestamp())

def process_data(parsed_df):
    """데이터 처리 및 변환"""
    # 1. 상태별 집계
    status_counts = parsed_df \
        .groupBy("status") \
        .count() \
        .orderBy(desc("count"))
    
    # 2. 10초 단위 윈도우 집계
    windowed_avg = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds", "5 seconds"),
            col("device_id")
        ) \
        .agg(
            avg("value").alias("avg_value"),
            count("*").alias("record_count")
        )
    
    return status_counts, windowed_avg
```

Spark 스트리밍 작업을 실행합니다(별도 터미널에서 실행):

```bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/spark-apps/basic_kafka_streaming.py
```

> **참고**: 타임스탬프 변환 방식이 문제가 될 경우, 스크립트 내에서 `to_timestamp(col("timestamp"))`를 `col("timestamp").cast("timestamp")`로 수정해야 합니다.

Spark UI를 확인하여 작업 상태를 모니터링합니다:
```
http://localhost:8181
```

실행 후 콘솔에 표시되는 결과는 다음과 유사합니다:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+------+-----+
|status|count|
+------+-----+
|Normal|35   |
|Danger|12   |
|Warning|8    |
|Error |5    |
+------+-----+

-------------------------------------------
Batch: 0
-------------------------------------------
+------------------+------------------+---------+------------------+------------+
|window_start      |window_end        |device_id|avg_value         |record_count|
+------------------+------------------+---------+------------------+------------+
|2025-04-20 14:32:55|2025-04-20 14:33:05|device_001|56.43            |4          |
|2025-04-20 14:32:55|2025-04-20 14:33:05|device_002|63.21            |3          |
|2025-04-20 14:32:55|2025-04-20 14:33:05|device_003|61.125           |4          |
+------------------+------------------+---------+------------------+------------+
```

### 5단계: 결과 확인 및 분석

처리된 결과를 확인합니다:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic processed_data_topic --from-beginning --max-messages 5
```

결과 예시:
```json
{"window_start":"2025-04-20T14:32:55.000Z","window_end":"2025-04-20T14:33:05.000Z","device_id":"device_003","avg_value":61.125,"record_count":4}
```

생성된 파일 출력 결과를 확인합니다:

```bash
docker exec -it spark-master ls -la /tmp/output/streaming_data
```

## 시나리오 2: 윈도우 기반 집계 및 이상 감지

### 1단계: 추가 토픽 설정

윈도우 집계 결과용 토픽을 생성합니다:

```bash
docker exec -it kafka kafka-topics --create --topic windowed_results_topic --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
```

이상 감지 알림용 토픽을 생성합니다:

```bash
docker exec -it kafka kafka-topics --create --topic anomaly_alerts_topic --bootstrap-server kafka:29092 --partitions 3 --replication-factor 1
```

### 2단계: 체크포인트 디렉토리 생성

체크포인트 디렉토리를 생성합니다:

```bash
docker exec -it spark-master mkdir -p /tmp/checkpoint/windowed-kafka
docker exec -it spark-master mkdir -p /tmp/checkpoint/anomaly-alerts
```

### 3단계: 테스트 데이터 생성

데이터 생성기를 실행하여 이상값을 포함한 테스트 데이터를 생성합니다:

```bash
# 기존에 실행되고 있지 않은 경우에만 실행
docker exec -it spark-master python /opt/spark-apps/data_generator.py
```

> **참고**: 데이터 생성기는 10% 확률로 200~500 범위의 이상값을 생성합니다.

### 4단계: 윈도우 기반 집계 실습

`windowed_aggregation.py` 스크립트의 주요 코드는 다음과 같습니다:

```python
def parse_data(kafka_df):
    """JSON 메시지 파싱"""
    # 스키마 정의
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("device_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # JSON 파싱 및 타임스탬프 변환
    return kafka_df \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .select(
            col("key"),
            from_json(col("value"), schema).alias("data")
        ) \
        .select(
            col("key"),
            col("data.device_id"),
            col("data.status"),
            col("data.value"),
            to_timestamp(col("data.timestamp")).alias("event_time")
        )

def create_windowed_aggregations(df_with_watermark):
    """다양한 윈도우 기반 집계 생성"""
    
    # 1. 고정 윈도우(Tumbling Window) - 10초 단위
    tumbling_window_agg = df_with_watermark \
        .groupBy(
            window(col("event_time"), "10 seconds"),
            col("device_id")
        ) \
        .agg(
            avg("value").alias("avg_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value"),
            count("*").alias("count")
        )
    
    # 2. 슬라이딩 윈도우(Sliding Window) - 30초 윈도우, 10초 간격
    sliding_window_agg = df_with_watermark \
        .groupBy(
            window(col("event_time"), "30 seconds", "10 seconds"),
            col("device_id")
        ) \
        .agg(
            avg("value").alias("avg_value"),
            stddev("value").alias("stddev_value")
        )
    
    # 3. 시간대별 집계 (1시간 단위)
    hourly_agg = df_with_watermark \
        .groupBy(
            window(col("event_time"), "1 hour"),
            col("status")
        ) \
        .count()
    
    return tumbling_window_agg, sliding_window_agg, hourly_agg
```

별도 터미널에서 윈도우 기반 집계 애플리케이션을 실행합니다:

```bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/spark-apps/windowed_aggregation.py
```

> **중요**: 실행 전 `windowed_aggregation.py` 스크립트에서 타임스탬프 변환 방식이 `col("data.timestamp").cast("timestamp")`로 수정되어 있는지 확인하세요.

윈도우 집계 결과를 확인합니다:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic windowed_results_topic --from-beginning --max-messages 3
```

실행 후 콘솔에 표시되는 결과는 다음과 유사합니다:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------+------------------+---------+------------------+------------+------------------+------------------+
|window_start      |window_end        |device_id|avg_value         |record_count|min_value         |max_value         |
+------------------+------------------+---------+------------------+------------+------------------+------------------+
|2025-04-20 14:33:20|2025-04-20 14:33:30|device_001|58.35            |5          |45.21             |78.92             |
|2025-04-20 14:33:20|2025-04-20 14:33:30|device_002|61.43            |4          |43.76             |83.21             |
+------------------+------------------+---------+------------------+------------+------------------+------------------+

-------------------------------------------
Batch: 0
-------------------------------------------
+------------------+------------------+---------+------------------+------------+------------------+------------------+
|window_start      |window_end        |device_id|avg_value         |record_count|min_value         |max_value         |
+------------------+------------------+---------+------------------+------------+------------------+------------------+
|2025-04-20 14:33:00|2025-04-20 14:33:30|device_001|64.72            |9          |43.21             |85.92             |
|2025-04-20 14:33:00|2025-04-20 14:33:30|device_003|59.83            |7          |44.67             |78.34             |
+------------------+------------------+---------+------------------+------------+------------------+------------------+
```

### 5단계: 이상 감지 실습

`anomaly_detection_streaming.py` 스크립트의 주요 코드는 다음과 같습니다:

```python
def parse_data(kafka_df):
    """JSON 메시지 파싱"""
    # 스키마 정의
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("device_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # JSON 파싱 및 타임스탬프 변환
    return kafka_df \
        .selectExpr("CAST(key AS STRING) as message_key", "CAST(value AS STRING) as message_value") \
        .select(
            col("message_key").alias("key"),
            from_json(col("message_value"), schema).alias("data")
        ) \
        .select(
            col("key"),
            col("data.device_id"),
            col("data.status"),
            col("data.value"),
            to_timestamp(col("data.timestamp")).alias("event_time")
        ) \
        .withColumn("processing_time", current_timestamp())

def detect_anomalies(df, window_duration="30 seconds", slide_duration="10 seconds", threshold_sigma=2.0):
    """
    슬라이딩 윈도우 기반 통계적 이상 감지
    
    z-점수(표준 편차 몇 배 벗어났는지) 기반으로 이상치 감지:
    z = (x - μ) / σ
    
    여기서:
    - x: 현재 값
    - μ: 윈도우 내 평균
    - σ: 윈도우 내 표준 편차
    
    threshold_sigma를 초과하는 경우 이상으로 감지
    """
    # 윈도우 정의
    windowedDF = df \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(
            window(col("event_time"), window_duration, slide_duration),
            col("device_id")
        ) \
        .agg(
            count("*").alias("count"),
            avg("value").alias("avg_value"),
            stddev("value").alias("stddev_value"),
            max("value").alias("max_value"),
            min("value").alias("min_value"),
            collect_list("value").alias("values")
        )
    
    # 이상 감지 로직 적용
    anomaliesDF = windowedDF \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device_id"),
            col("count"),
            col("avg_value"),
            col("stddev_value"),
            col("max_value"),
            col("min_value"),
        )
    
    # z-점수 계산 및 이상 감지
    anomaliesDF = anomaliesDF.withColumn(
        "stddev_value", 
        when(col("stddev_value").isNull() | (col("stddev_value") == 0), 
             lit(0.001)).otherwise(col("stddev_value"))
    ).withColumn(
        "z_score_max", 
        (col("max_value") - col("avg_value")) / col("stddev_value")
    ).withColumn(
        "z_score_min", 
        (col("min_value") - col("avg_value")) / col("stddev_value")
    ).withColumn(
        "is_anomaly", 
        (col("z_score_max") > threshold_sigma) | (col("z_score_min") < -threshold_sigma)
    )
    
    return anomaliesDF
```

별도 터미널에서 이상 감지 애플리케이션을 실행합니다:

```bash
docker exec -it spark-master spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 /opt/spark-apps/anomaly_detection_streaming.py
```

> **중요**: 실행 전 `anomaly_detection_streaming.py` 스크립트에서 타임스탬프 변환 방식이 `col("data.timestamp").cast("timestamp")`로 수정되어 있는지 확인하세요.

이상 감지 알림을 확인합니다:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic anomaly_alerts_topic --from-beginning --timeout-ms 5000
```

실행 후 콘솔에 표시되는 이상 감지 결과는 다음과 유사합니다:

```
-------------------------------------------
Batch: 0
-------------------------------------------
+------------------+------------------+---------+-----------------+------------+
|window_start      |window_end        |device_id|anomaly_type     |anomaly_score|
+------------------+------------------+---------+-----------------+------------+
|2025-04-20 14:35:10|2025-04-20 14:35:40|device_002|HIGH_VALUE      |3.85        |
+------------------+------------------+---------+-----------------+------------+
```

### 6단계: 체크포인트 상태 확인

체크포인트 디렉토리의 상태를 확인합니다:

```bash
docker exec -it spark-master ls -la /tmp/checkpoint/
```

## 트러블슈팅

### 연결 문제

**문제**: Kafka 브로커 연결 실패 오류가 발생하는 경우

**해결방법**: 스크립트 내에서 Kafka 브로커 주소가 올바르게 설정되어 있는지 확인합니다.

```python
# 내부 통신용 주소 사용
.option("kafka.bootstrap.servers", "kafka:29092")
```

### 프로세스 관리

실행 중인 프로세스를 확인하고 필요시 종료합니다:

```bash
# 실행 중인 Python 프로세스 확인
docker exec -it spark-master ps -ef | grep python

# 프로세스 종료
docker exec -it spark-master kill -9 <PID>
```

## 결론

이 실습을 통해 Kafka와 Spark Structured Streaming을 사용한 실시간 데이터 처리 파이프라인의 구축 방법을 배웠습니다. 기본 스트리밍, 윈도우 기반 집계, 이상 감지의 세 가지 다른 유형의 처리 작업을 구현하며 실시간 분석의 핵심 개념을 익혔습니다.
