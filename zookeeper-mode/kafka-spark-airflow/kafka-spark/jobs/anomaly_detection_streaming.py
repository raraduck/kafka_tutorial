"""
Kafka 데이터 스트림에서 실시간 이상 감지를 수행하는 Spark Structured Streaming 애플리케이션

이 스크립트는 다음 기능을 수행합니다:
1. Kafka 토픽에서 IoT 디바이스 데이터 스트림 수신
2. 슬라이딩 윈도우를 사용한 통계 기반 이상 감지
3. 이상 감지 결과를 다른 Kafka 토픽으로 전송
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window


def create_spark_session():
    """Kafka 연결용 설정을 포함한 SparkSession 생성"""
    return SparkSession \
        .builder \
        .appName("AnomalyDetectionStreaming") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()


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
    
    # stddev가 0이면 나누기 오류 방지를 위해 null 대신 작은 값 사용
    anomaliesDF = anomaliesDF.withColumn(
        "stddev_value", 
        when(col("stddev_value").isNull() | (col("stddev_value") == 0), 
             lit(0.001)).otherwise(col("stddev_value"))
    )
    
    # z-점수 계산
    anomaliesDF = anomaliesDF.withColumn(
        "z_score_max", 
        (col("max_value") - col("avg_value")) / col("stddev_value")
    ).withColumn(
        "z_score_min", 
        (col("min_value") - col("avg_value")) / col("stddev_value")
    )
    
    # 이상 감지
    anomaliesDF = anomaliesDF.withColumn(
        "is_anomaly_high", 
        col("z_score_max") > threshold_sigma
    ).withColumn(
        "is_anomaly_low", 
        col("z_score_min") < -threshold_sigma
    ).withColumn(
        "is_anomaly", 
        col("is_anomaly_high") | col("is_anomaly_low")
    ).withColumn(
        "anomaly_type", 
        when(col("is_anomaly_high"), "HIGH_VALUE")
        .when(col("is_anomaly_low"), "LOW_VALUE")
        .otherwise("NORMAL")
    ).withColumn(
        "anomaly_score", 
        when(col("is_anomaly_high"), col("z_score_max"))
        .when(col("is_anomaly_low"), abs(col("z_score_min")))
        .otherwise(lit(0.0))
    )
    
    return anomaliesDF


def process_device_data(parsed_df):
    """각 디바이스별 데이터 처리 및 이상 감지"""
    # 디바이스별 상태 정보
    device_status = parsed_df \
        .groupBy("device_id", "status") \
        .count() \
        .orderBy(col("device_id"), desc("count"))
    
    # 이상 감지
    anomalies = detect_anomalies(parsed_df)
    
    # 이상만 필터링
    alert_anomalies = anomalies \
        .filter(col("is_anomaly") == True) \
        .select(
            col("window_start"),
            col("window_end"),
            col("device_id"),
            col("anomaly_type"),
            col("anomaly_score"),
            col("avg_value"),
            col("max_value"),
            col("min_value")
        )
    
    return device_status, anomalies, alert_anomalies


def start_queries(device_status, anomalies, alert_anomalies):
    """스트리밍 쿼리 시작"""
    # 디바이스 상태 콘솔 출력
    query1 = device_status \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 모든 윈도우 결과 (메모리 테이블에 저장)
    query2 = anomalies \
        .writeStream \
        .queryName("anomalies_table") \
        .outputMode("complete") \
        .format("memory") \
        .start()
    
    # 이상 알림만 콘솔 출력
    query3 = alert_anomalies \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    return [query1, query2, query3]


def write_alerts_to_kafka(alert_anomalies, output_topic):
    """이상 감지 알림을 Kafka로 전송"""
    return alert_anomalies \
        .select(
            col("device_id").alias("key"),
            to_json(struct(
                col("window_start"),
                col("window_end"),
                col("device_id"),
                col("anomaly_type"),
                col("anomaly_score"),
                col("avg_value"),
                col("max_value"),
                col("min_value"),
                current_timestamp().alias("alert_time")
            )).alias("value")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/checkpoint/anomaly-alerts") \
        .outputMode("append") \
        .start()


def main():
    """메인 실행 함수"""
    # SparkSession 생성
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Kafka에서 데이터 읽기
    input_topic = "stream_data_topic"
    output_topic = "anomaly_alerts_topic"
    kafka_df = read_from_kafka(spark, input_topic)
    
    # 데이터 파싱
    parsed_df = parse_data(kafka_df)
    
    # 데이터 처리 및 이상 감지
    device_status, anomalies, alert_anomalies = process_device_data(parsed_df)
    
    # 스트리밍 쿼리 시작
    queries = start_queries(device_status, anomalies, alert_anomalies)
    
    # 이상 알림을 Kafka로 전송
    kafka_alert_query = write_alerts_to_kafka(alert_anomalies, output_topic)
    queries.append(kafka_alert_query)
    
    # 모든 쿼리가 종료될 때까지 대기
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main() 