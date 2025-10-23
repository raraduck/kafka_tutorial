# Airflow + Spark (batch) + Kafka + Postgres

This project automates a **Kafka → Spark (batch) → Postgres** pipeline via **Airflow** every **5 minutes**.
It demonstrates **Spark submission**:
1) via `SparkSubmitOperator`

After ingestion, a Postgres step **re-aggregates** minute-level counts.

---

## 1) Prereqs
- Docker & Docker Compose

## 2) Quickstart

### 1. Create Cluster

```bash
docker compose up -d
```

### 2. Create Topic
```bash
./create_topic.sh user_events 3 2
```

### 3. Produce Messages
```bash
python kafka/producer.py
```

### 4. spark 서버에서 연결 테스트
> 성공
> ```bash
> spark-submit \
> --master spark://spark-master:7077 \
> --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
> /opt/spark-apps/consume_kafka_to_postgres_batch.py
> ```

### 5. airflow 서버에서 연결 테스트 (local모드로 해야하는 이유?)

> 실패
> ```bash
> spark-submit \
> --master spark://spark-master:7077 \
> --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
> /opt/spark-apps/consume_kafka_to_postgres_batch.py
> ```

> 실패
> ```bash
> spark-submit \
> --master spark://spark-master:7077 \
> --deploy-mode client \
> --conf spark.driver.host=airflow-webserver \
> --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
> /opt/spark-apps/consume_kafka_to_postgres_batch.py
> ```

> 성공
> ```bash 
> spark-submit \
> --master local[*] \
> --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
> /opt/spark-apps/consume_kafka_to_postgres_batch.py 
> ```

### 6. airflow 에 connection 추가 (local mode and spark_default)
```bash
airflow connections add 'spark_local' \
    --conn-type 'spark' \
    --conn-host 'local[*]'

airflow connections add 'spark_default' \
    --conn-type 'spark' \
    --conn-host 'spark://spark-master' \
    --conn-port '7077'
```
