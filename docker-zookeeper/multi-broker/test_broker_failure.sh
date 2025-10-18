#!/bin/bash

# 색상 정의
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 토픽 이름
TOPIC_NAME=${1:-"orders"}

# 브로커 중단 및 복구 함수
stop_broker() {
    local broker_id=$1
    echo -e "${RED}브로커 $broker_id 중단 중...${NC}"
    docker stop kafka$broker_id
    echo -e "${RED}브로커 $broker_id 중단됨${NC}"
}

start_broker() {
    local broker_id=$1
    echo -e "${GREEN}브로커 $broker_id 복구 중...${NC}"
    docker start kafka$broker_id
    echo -e "${GREEN}브로커 $broker_id 복구됨${NC}"
    
    # 브로커 상태 확인
    echo -e "${BLUE}브로커 $broker_id 상태 확인 중...${NC}"
    sleep 5 # 브로커가 완전히 시작될 때까지 대기
    docker exec -it kafka$broker_id kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}브로커 $broker_id가 정상적으로 작동 중입니다.${NC}"
    else
        echo -e "${RED}브로커 $broker_id가 아직 준비되지 않았습니다.${NC}"
    fi
}

# 토픽 파티션 상태 확인 함수
check_topic() {
    echo -e "${YELLOW}토픽 '$TOPIC_NAME' 상태 확인 중...${NC}"
    docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic $TOPIC_NAME
}

# 시나리오 1: 단일 브로커 장애 테스트
test_single_broker_failure() {
    echo -e "\n${BLUE}==== 시나리오 1: 단일 브로커 장애 테스트 ====${NC}"
    echo -e "${BLUE}시작 상태의 토픽 정보:${NC}"
    check_topic
    
    echo -e "\n${BLUE}1. 브로커 3 중단${NC}"
    stop_broker 3
    sleep 5
    check_topic
    
    echo -e "\n${BLUE}2. 5초 후 브로커 3 복구${NC}"
    sleep 5
    start_broker 3
    sleep 10
    check_topic
    
    echo -e "\n${GREEN}시나리오 1 완료: 단일 브로커 장애 및 복구 테스트${NC}"
}

# 시나리오 2: 다중 브로커 장애 테스트 (정족수 미달)
test_multiple_broker_failure() {
    echo -e "\n${BLUE}==== 시나리오 2: 다중 브로커 장애 테스트 (정족수 미달) ====${NC}"
    echo -e "${BLUE}시작 상태의 토픽 정보:${NC}"
    check_topic
    
    echo -e "\n${BLUE}1. 브로커 2 중단${NC}"
    stop_broker 2
    sleep 5
    check_topic
    
    echo -e "\n${BLUE}2. 브로커 3 중단 (이제 1개 브로커만 작동)${NC}"
    stop_broker 3
    sleep 5
    check_topic
    
    echo -e "\n${BLUE}3. 브로커 복구 시작${NC}"
    start_broker 2
    sleep 10
    check_topic
    
    start_broker 3
    sleep 10
    check_topic
    
    echo -e "\n${GREEN}시나리오 2 완료: 다중 브로커 장애 및 복구 테스트${NC}"
}

# 시나리오 3: 리더 브로커 장애 테스트
test_leader_broker_failure() {
    echo -e "\n${BLUE}==== 시나리오 3: 리더 브로커 장애 테스트 ====${NC}"
    
    # 현재 리더 파악
    echo -e "${BLUE}현재 토픽 파티션 리더 확인:${NC}"
    leaders=$(docker exec -it kafka1 kafka-topics --bootstrap-server kafka1:29092 --describe --topic $TOPIC_NAME | grep Leader)
    echo "$leaders"
    
    # kafka1이 리더인 파티션이 있는지 확인
    if echo "$leaders" | grep -q "Leader: 1"; then
        echo -e "\n${BLUE}브로커 1이 리더인 파티션이 있습니다. 브로커 1을 중단합니다.${NC}"
        stop_broker 1
    else
        echo -e "\n${BLUE}브로커 1이 리더가 아닙니다. 대신 브로커 2를 중단합니다.${NC}"
        stop_broker 2
    fi
    
    sleep 10
    echo -e "\n${BLUE}리더 변경 확인:${NC}"
    docker exec -it $(docker ps -q -f name=kafka[^1]) kafka-topics --bootstrap-server kafka2:29093 --describe --topic $TOPIC_NAME | grep Leader
    
    echo -e "\n${BLUE}중단된 브로커 복구${NC}"
    if echo "$leaders" | grep -q "Leader: 1"; then
        start_broker 1
    else
        start_broker 2
    fi
    
    sleep 10
    echo -e "\n${BLUE}최종 토픽 상태:${NC}"
    check_topic
    
    echo -e "\n${GREEN}시나리오 3 완료: 리더 브로커 장애 및 복구 테스트${NC}"
}

# 메인 실행 부분
echo -e "${BLUE}Kafka 다중 브로커 장애 테스트 시나리오${NC}"
echo -e "${YELLOW}주의: 이 테스트는 실제 브로커를 중단시키므로 프로덕션 환경에서는 실행하지 마세요.${NC}"
echo -e "${GREEN}토픽: $TOPIC_NAME${NC}"

# 사용자 입력 받기
echo -e "\n다음 테스트 중 실행할 시나리오를 선택하세요:"
echo "1) 단일 브로커 장애 테스트"
echo "2) 다중 브로커 장애 테스트 (정족수 미달)"
echo "3) 리더 브로커 장애 테스트"
echo "4) 모든 시나리오 실행"
echo "q) 종료"

read -p "선택 (1-4 또는 q): " choice

case $choice in
    1) test_single_broker_failure ;;
    2) test_multiple_broker_failure ;;
    3) test_leader_broker_failure ;;
    4) 
        test_single_broker_failure
        sleep 5
        test_multiple_broker_failure
        sleep 5
        test_leader_broker_failure
        ;;
    q|Q) echo "테스트를 종료합니다." ;;
    *) echo "잘못된 선택입니다." ;;
esac

echo -e "\n${GREEN}테스트 완료. 모든 브로커가 정상 작동하는지 확인하세요.${NC}"
docker ps -a | grep kafka 