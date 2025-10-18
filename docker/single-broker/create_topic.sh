#!/bin/bash

# 토픽 이름
TOPIC_NAME=${1:-"user_activity"}
PARTITIONS=${2:-1}
REPLICATION_FACTOR=${3:-1}

echo "Creating topic '$TOPIC_NAME' with $PARTITIONS partition(s) and replication factor $REPLICATION_FACTOR"

# Kafka 컨테이너에서 토픽 생성 명령 실행
docker exec -it kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --create \
  --topic $TOPIC_NAME \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

# 토픽 확인
echo -e "\nVerifying created topic:"
docker exec -it kafka \
  kafka-topics --bootstrap-server localhost:9092 \
  --describe \
  --topic $TOPIC_NAME 