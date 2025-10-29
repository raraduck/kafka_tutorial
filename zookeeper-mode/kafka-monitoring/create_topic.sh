#!/bin/bash

if [ "$#" -lt 3 ]; then
    echo "사용법: $0 <토픽명> <파티션수> <복제팩터>"
    exit 1
fi

TOPIC=$1
PARTITIONS=$2
REPLICATION=$3

echo "토픽 '$TOPIC' 생성 중 (파티션: $PARTITIONS, 복제팩터: $REPLICATION)..."

docker exec -it kafka1 kafka-topics --create \
  --bootstrap-server kafka1:29092 \
  --topic $TOPIC \
  --partitions $PARTITIONS \
  --replication-factor $REPLICATION

echo "토픽 '$TOPIC' 생성 완료!"

# 토픽 정보 확인
docker exec -it kafka1 kafka-topics --describe \
  --bootstrap-server kafka1:29092 \
  --topic $TOPIC 