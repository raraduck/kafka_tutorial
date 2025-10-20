"""
시간 윈도우 기반 집계 및 스트림 처리를 수행하는 스크립트

이 스크립트는 다음 기능을 구현합니다:
1. 고정 윈도우(Tumbling window)와 슬라이딩 윈도우(Sliding window) 집계
2. 워터마크를 통한 지연 데이터 처리
3. 시간대별, 디바이스별 집계 지표 생성
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Kafka 연결용 설정을 포함한 SparkSession 생성"""
    return SparkSession \
        .builder \
        .appName("WindowedAggregation") \
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


def start_streaming_queries(tumbling_window_agg, sliding_window_agg, hourly_agg):
    """스트리밍 쿼리 시작"""
    
    # 쿼리 실행 - 고정 윈도우
    query1 = tumbling_window_agg \
        .writeStream \
        .queryName("tumbling_window") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 쿼리 실행 - 슬라이딩 윈도우
    query2 = sliding_window_agg \
        .writeStream \
        .queryName("sliding_window") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 쿼리 실행 - 시간대별 집계
    query3 = hourly_agg \
        .writeStream \
        .queryName("hourly_agg") \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    return [query1, query2, query3]


def write_window_results_to_kafka(tumbling_window_agg, topic):
    """윈도우 집계 결과를 Kafka로 전송"""
    return tumbling_window_agg \
        .selectExpr(
            "CAST(device_id AS STRING) AS key",
            "to_json(struct(window, device_id, avg_value, max_value, min_value, count)) AS value"
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", topic) \
        .option("checkpointLocation", "/tmp/checkpoint/windowed-kafka") \
        .outputMode("update") \
        .start()


def main():
    """메인 실행 함수"""
    # SparkSession 생성
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Kafka에서 데이터 읽기
    input_topic = "stream_data_topic"
    output_topic = "windowed_results_topic"
    kafka_df = read_from_kafka(spark, input_topic)
    
    # 데이터 파싱
    parsed_df = parse_data(kafka_df)
    
    # 워터마크 설정 - 1분까지의 지연 데이터 처리
    df_with_watermark = parsed_df \
        .withWatermark("event_time", "1 minute")
    
    # 윈도우 기반 집계 생성
    tumbling_window_agg, sliding_window_agg, hourly_agg = create_windowed_aggregations(df_with_watermark)
    
    # 콘솔 출력 쿼리 시작
    queries = start_streaming_queries(tumbling_window_agg, sliding_window_agg, hourly_agg)
    
    # Kafka로 결과 전송 (선택적)
    kafka_query = write_window_results_to_kafka(tumbling_window_agg, output_topic)
    queries.append(kafka_query)
    
    # 모든 쿼리가 종료될 때까지 대기
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main() 