# Metacode Project in 8 week

## Steps
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

### 4. 
```bash
spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
/opt/spark-apps/consume_kafka_to_postgres_batch.py 

# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0 \
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
```