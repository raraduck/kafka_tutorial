"""
Kafka 데이터를 Spark Structured Streaming으로 처리하는 기본 예제

이 스크립트는 Kafka 토픽(stream_data_topic)에서 데이터를 소비하고
Spark Structured Streaming을 사용하여 실시간으로 처리합니다.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    """Kafka 연결용 설정을 포함한 SparkSession 생성"""
    return SparkSession \
        .builder \
        .appName("BasicKafkaStreaming") \
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
    # 예시 스키마 정의 (실제 데이터에 맞게 수정 필요)
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("device_id", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # JSON 파싱 및 타임스탬프 추가
    return kafka_df \
        .selectExpr("CAST(key AS STRING) as message_key", "CAST(value AS STRING) as message_value") \
        .select(
            col("message_key"),
            from_json(col("message_value"), schema).alias("data")
        ) \
        .select("message_key", "data.*") \
        .withColumn("timestamp", col("timestamp").cast("timestamp")) \
        .withColumn("processing_time", current_timestamp())


def process_data(parsed_df):
    """데이터 처리 및 변환"""
    # 1. 상태별 집계
    status_counts = parsed_df \
        .groupBy("status") \
        .count() \
        .orderBy(desc("count"))
    
    # 2. 10초 단위 윈도우 집계
    windowed_avg = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "10 seconds", "5 seconds"),
            col("device_id")
        ) \
        .agg(
            avg("value").alias("avg_value"),
            count("*").alias("record_count")
        )
    
    return status_counts, windowed_avg


def start_streaming_queries(status_counts, windowed_avg):
    """스트리밍 쿼리 시작"""
    # 상태별 집계 콘솔 출력
    query1 = status_counts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 윈도우 집계 콘솔 출력
    query2 = windowed_avg \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    return [query1, query2]


def write_to_kafka(windowed_avg, output_topic):
    """처리 결과를 다른 Kafka 토픽으로 전송"""
    # 결과를 JSON으로 변환하여 Kafka에 전송
    return windowed_avg \
        .select(
            lit("result").alias("key"),
            to_json(struct(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("device_id"),
                col("avg_value"),
                col("record_count")
            )).alias("value")
        ) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/checkpoint/kafka-to-kafka") \
        .outputMode("append") \
        .start()


def write_to_file(processed_df, checkpoint_dir, path):
    """처리 결과를 파일로 저장"""
    return processed_df \
        .writeStream \
        .format("parquet") \
        .option("path", path) \
        .option("checkpointLocation", checkpoint_dir) \
        .outputMode("append") \
        .start()


def main():
    """메인 실행 함수"""
    # SparkSession 생성
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Kafka에서 데이터 읽기
    input_topic = "stream_data_topic"
    output_topic = "processed_data_topic"
    kafka_df = read_from_kafka(spark, input_topic)
    
    # 데이터 파싱
    parsed_df = parse_data(kafka_df)
    
    # 데이터 처리
    status_counts, windowed_avg = process_data(parsed_df)
    
    # 스트리밍 쿼리 시작
    queries = start_streaming_queries(status_counts, windowed_avg)
    
    # Kafka로 결과 전송 (선택적)
    kafka_output_query = write_to_kafka(windowed_avg, output_topic)
    queries.append(kafka_output_query)
    
    # 파일로 결과 저장 (선택적)
    file_output_query = write_to_file(
        parsed_df, 
        "/tmp/checkpoint/file-output",
        "/tmp/output/streaming_data"
    )
    queries.append(file_output_query)
    
    # 모든 쿼리가 종료될 때까지 대기
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main() 