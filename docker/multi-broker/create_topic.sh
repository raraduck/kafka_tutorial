#!/bin/bash

# 토픽 이름
TOPIC_NAME=${1:-"orders"}
PARTITIONS=${2:-3}
REPLICATION_FACTOR=${3:-3}

echo "Creating topic '$TOPIC_NAME' with $PARTITIONS partition(s) and replication factor $REPLICATION_FACTOR"

# Kafka 컨테이너에서 토픽 생성 명령 실행
docker exec -it kafka1 \
  kafka-topics --bootstrap-server kafka1:29092,kafka2:29093,kafka3:29094 \
  --create \
  --topic $TOPIC_NAME \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION_FACTOR

# 토픽 확인
echo -e "\nVerifying created topic:"
docker exec -it kafka1 \
  kafka-topics --bootstrap-server kafka1:29092 \
  --describe \
  --topic $TOPIC_NAME

# 컨슈머 그룹 생성 (선택 사항)
if [ "$4" = "with-group" ]; then
  GROUP_ID="order_processing_group"
  echo -e "\nCreating consumer group '$GROUP_ID'"
  
  # 컨슈머 그룹 생성 (간단히 한 번 실행하고 중단)
  docker exec -it kafka1 \
    kafka-console-consumer --bootstrap-server kafka1:29092 \
    --topic $TOPIC_NAME \
    --group $GROUP_ID \
    --from-beginning \
    --max-messages 1
    
  # 컨슈머 그룹 정보 확인
  echo -e "\nVerifying consumer group:"
  docker exec -it kafka1 \
    kafka-consumer-groups --bootstrap-server kafka1:29092 \
    --describe \
    --group $GROUP_ID
fi 