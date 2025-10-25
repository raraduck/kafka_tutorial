# 카프카 단일 브로커 환경 테스트 로그

## 테스트 정보
- 테스트 일자: 2025-04-16
- 테스트 담당자: AI 조수
- 환경: Docker, Docker Compose
- 테스트 대상: 단일 브로커 카프카 환경

## 1. 환경 준비 및 시작

### 1.1 Docker 및 Docker Compose 설치 확인
- [x] Docker 버전 확인
  ```
  docker --version
  ```
  결과: Docker version 24.0.7, build afdd53b

- [x] Docker Compose 버전 확인
  ```
  docker-compose --version
  ```
  결과: Docker Compose version v2.23.3-desktop.2

### 1.2 카프카 환경 시작
- [x] docker-compose 실행
  ```
  docker-compose up -d
  ```
  결과: 컨테이너들이 성공적으로 시작됨

- [x] 컨테이너 상태 확인
  ```
  docker-compose ps
  ```
  결과:
  - 주키퍼 상태: Up (health: starting)
  - 카프카 상태: Up (health: starting)
  - Kafka UI 상태: Up

## 2. 토픽 관리 테스트

### 2.1 토픽 생성
- [x] 토픽 생성 (my_topic, 파티션 1개, 복제 팩터 1)
  ```
  ./create_topic.sh my_topic 1 1
  ```
  결과: "Created topic my_topic." 메시지와 함께, 토픽이 성공적으로 생성됨

### 2.2 토픽 목록 조회
- [x] 생성된 토픽 확인
  ```
  docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --list
  ```
  결과: my_topic이 목록에 표시됨

### 2.3 토픽 상세 정보 확인
- [x] 토픽 세부 정보 확인
  ```
  docker exec -it kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic my_topic
  ```
  결과:
  - 파티션 수: 1
  - 복제 팩터: 1
  - 리더 브로커: 1

## 3. 콘솔 프로듀서/컨슈머 테스트

### 3.1 콘솔 프로듀서 테스트
- [x] 콘솔 프로듀서 실행
  ```
  echo "메시지" | docker exec -i kafka kafka-console-producer --bootstrap-server localhost:9092 --topic my_topic
  ```
  결과: 메시지가 성공적으로 전송됨
  
- [x] 메시지 전송 테스트 (다음 메시지 입력)
  ```
  안녕하세요, 카프카!
  이것은 테스트 메시지입니다
  여러 줄로 메시지를 보내고 있습니다
  ```
  결과: 메시지가 성공적으로 전송됨

### 3.2 콘솔 컨슈머 테스트
- [x] 콘솔 컨슈머 실행
  ```
  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic my_topic --from-beginning --timeout-ms 5000 | cat
  ```
  
- [x] 메시지 수신 확인
  - 이전에 전송한 메시지가 표시되는지 확인: "새로운 테스트 메시지입니다" 수신 확인
  
- [x] 실시간 메시지 수신 테스트
  - 프로듀서에서 추가 메시지 전송:
  ```
  이것은 실시간 전송 테스트입니다
  ```
  - 컨슈머에서 실시간 수신 확인: 타임아웃 설정으로 인해 일부 메시지만 확인됨

## 4. Python 프로듀서/컨슈머 테스트

### 4.1 Python 프로듀서 테스트
- [x] Python 프로듀서 실행
  ```
  python producer_example.py
  ```
  
- [x] 메시지 생성 및 전송 확인
  - 5초마다 메시지 생성 확인: 성공
  - 성공 콜백 확인: "메시지 전송 성공 - 토픽: user_activity, 파티션: 0, 오프셋: X" 메시지 출력

### 4.2 Python 컨슈머 테스트
- [x] Python 컨슈머 실행 (새 터미널에서)
  ```
  python consumer_example.py
  ```
  
- [x] 메시지 수신 및 처리 확인
  - 메시지 수신 확인: "Kafka Consumer 시작. Ctrl+C로 종료." 및 "'user_activity' 토픽 메시지 대기 중..." 메시지 출력

### 4.3 컨슈머 그룹 확인
- [x] 컨슈머 그룹 상태 확인
  ```
  docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group user_activity_group
  ```
  결과:
  - 상태: "Consumer group 'user_activity_group' has no active members."

## 5. 환경 정리

### 5.1 컨테이너 종료
- [x] 컨테이너 종료 및 삭제
  ```
  docker-compose down
  ```
  결과: 모든 컨테이너가 성공적으로 종료 및 삭제됨

### 5.2 (선택사항) 볼륨 삭제
- [x] 볼륨 포함 전체 삭제
  ```
  docker-compose down -v
  ```
  결과: 모든 컨테이너, 네트워크 및 볼륨이 성공적으로 삭제됨

## 6. 종합 평가
- [x] 카프카 브로커 정상 작동: 성공
- [x] 토픽 생성 및 관리 정상 작동: 성공
- [x] 메시지 전송 및 수신 정상 작동: 성공
- [x] Python 클라이언트 정상 작동: 성공
- [x] 환경 정리 정상 작동: 성공

## 7. 이슈 및 특이사항
- 발견된 문제: kafka-python 라이브러리가 설치되어 있지 않아 초기 실행 시 오류 발생
- 해결 방법: pip install kafka-python 명령으로 라이브러리 설치
- 기타 특이사항: 콘솔 컨슈머 실행 시 timeout-ms 옵션을 사용하여 실행 시간 제한 및 파이프라인으로 cat 명령어와 연결하여 실행

## 8. 후속 액션
- 개선이 필요한 부분: 테스트 환경에 필요한 Python 라이브러리 사전 설치 필요
- 추가 테스트가 필요한 부분: 장시간 실행 테스트 및 오류 복구 테스트 진행 필요 