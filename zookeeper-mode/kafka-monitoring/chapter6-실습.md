# Kafka 성능 최적화 실습

## 사전 준비사항
- Docker Compose 환경이 실행 중이어야 합니다
- Python 3.x 이상이 설치되어 있어야 합니다
- confluent-kafka-python 라이브러리가 설치되어 있어야 합니다

## 1. 압축 알고리즘 성능 비교 테스트
### 테스트 환경
- 클러스터 구성: 3노드 클러스터
- Replication Factor: 3
- 테스트 메시지 크기: 1KB, 10KB, 100KB
- 측정 지표: 처리량(MB/s), 지연시간(ms), CPU 사용률(%)

### 1.1 테스트 실행 방법

```bash
# 테스트용 토픽 생성
for compression in none gzip snappy lz4 zstd; do
    docker exec -it kafka1 kafka-topics --create \
        --bootstrap-server kafka1:29092 \
        --topic perf-test-$compression \
        --partitions 3 \
        --replication-factor 3
done

# 성능 테스트 실행 (1KB 메시지)
for compression in none gzip snappy lz4 zstd; do
    echo "Testing compression type: $compression"
    docker exec -it kafka1 kafka-producer-perf-test \
        --topic perf-test-$compression \
        --num-records 1000000 \
        --record-size 1024 \
        --throughput -1 \
        --producer-props bootstrap.servers=kafka1:29092 \
        compression.type=$compression \
        batch.size=128000 \
        linger.ms=10
    echo "--------------------------"
done
```

### 1.2 압축 알고리즘별 특성 비교 결과

| 압축 타입 | 처리량 (MB/s) | 평균 지연시간 (ms) | CPU 사용률 | 메모리 사용량 | 권장 사용 사례 |
|----------|--------------|-----------------|-----------|------------|------------|
| none     | 91.64       | 37.73          | 낮음      | 낮음       | 이미 압축된 데이터(이미지, 영상 등), 네트워크 대역폭이 충분한 경우 |
| gzip     | 29.36       | 5.88           | 높음      | 중간       | 높은 압축률이 필요하고 처리량이 중요하지 않은 로그 데이터 |
| snappy   | 96.21       | 6.78           | 중간      | 낮음       | 일반적인 JSON/텍스트 데이터, 높은 처리량이 필요한 경우 |
| lz4      | 88.66       | 6.38           | 낮음      | 낮음       | 실시간 처리가 필요한 데이터, 안정적인 처리량이 필요한 경우 |
| zstd     | 87.84       | 2.66           | 중간      | 중간       | 낮은 지연시간이 중요한 경우, 안정적인 성능이 필요한 경우 |

### 1.3 결과 해석 방법
- **처리량(MB/s)**: 높을수록 좋음, 초당 처리되는 데이터 양
- **CPU 사용률**: 낮을수록 서버 리소스 여유
- **메모리 사용량**: 낮을수록 좋음, 특히 대량의 데이터 처리 시 중요
- **압축률**: 높을수록 저장 공간 절약, but CPU 부하 증가

## 2. 배치 설정 성능 비교
### 2.1 테스트 파라미터
- batch.size: 프로듀서가 한 번에 전송할 수 있는 최대 크기
- linger.ms: 배치를 모으기 위해 대기하는 시간
- compression.type: 데이터 압축 방식

### 2.2 테스트 실행

```bash
# 테스트용 토픽 생성
docker exec -it kafka1 kafka-topics --create \
    --bootstrap-server kafka1:29092 \
    --topic perf-test-batch \
    --partitions 3 \
    --replication-factor 3

# 배치 설정별 성능 테스트
for batch_size in 16384 65536 131072; do
    for linger_ms in 0 10 50 100; do
        echo "Testing batch.size=$batch_size, linger.ms=$linger_ms"
        docker exec -it kafka1 kafka-producer-perf-test \
            --topic perf-test-batch \
            --num-records 1000000 \
            --record-size 1024 \
            --throughput -1 \
            --producer-props \
                bootstrap.servers=kafka1:29092 \
                batch.size=$batch_size \
                linger.ms=$linger_ms \
                compression.type=snappy
        echo "--------------------------"
    done
done
```

### 2.3 테스트를 통한 최적의 배치 설정 가이드라인
1. **높은 처리량이 필요한 경우**
   - batch.size: 128KB (131072)
   - linger.ms: 50-100ms
   - 예상 성능: ~100 MB/sec
   - 적합한 사용 사례: 대량 데이터 처리, ETL 작업

2. **낮은 지연시간이 필요한 경우**
   - batch.size: 64KB (65536)
   - linger.ms: 10ms
   - 예상 성능: ~98 MB/sec, 평균 지연시간 1.5ms
   - 적합한 사용 사례: 실시간 처리, 사용자 상호작용

3. **균형잡힌 설정 (처리량/지연시간)**
   - batch.size: 64KB-128KB
   - linger.ms: 50ms
   - 예상 성능: 96-99 MB/sec, 평균 지연시간 2-3ms
   - 적합한 사용 사례: 일반적인 메시징 워크로드

주의사항:
- 16KB 배치 크기는 성능이 현저히 떨어지므로 권장하지 않음
- linger.ms=0 설정은 지연시간 변동성이 크므로 피하는 것이 좋음
- 실제 환경에서는 메시지 크기와 패턴에 따라 추가 튜닝이 필요할 수 있음

## 3. 성능 모니터링
테스트 중 다음 지표들을 모니터링하면서 최적의 설정을 찾습니다:

1. **Kafka Manager/UI에서 확인할 지표**
   - 브로커별 초당 메시지 수
   - 파티션별 지연(lag) 상태
   - 브로커 CPU/메모리 사용률

2. **프로듀서 애플리케이션에서 확인할 지표**
   - 평균 지연시간 (avg latency)
   - 처리량 (throughput)
   - 실패율 (error rate)

## 4. 실습 결과 정리
각 테스트 후 아래 템플릿을 사용하여 결과를 정리하세요:

```
테스트 구성:
- 메시지 크기: [크기]
- 배치 설정: batch.size=[값], linger.ms=[값]
- 압축 방식: [방식]

측정 결과:
- 처리량: [값] MB/s
- 평균 지연시간: [값] ms
- CPU 사용률: [값] %
- 메모리 사용량: [값] MB

특이사항:
- [관찰된 특이사항 기록]
```

## 참고사항
- 실제 프로덕션 환경에서는 위 테스트 결과를 참고하되, 실제 워크로드와 요구사항에 맞게 조정이 필요합니다.
- 네트워크 상태, 디스크 I/O, 브로커 설정 등 다양한 요소가 성능에 영향을 미칠 수 있습니다.
- 정기적인 모니터링과 튜닝이 필요할 수 있습니다.