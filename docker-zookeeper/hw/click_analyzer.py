# 문제 3 & 4: Spark Streaming을 이용한 실시간 클릭 데이터 분석 및 패턴 감지
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, desc, from_json, countDistinct, to_timestamp, udf, approx_count_distinct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType
from kafka import KafkaProducer # 패턴 감지 시 Kafka로 재전송하기 위함
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka Producer 설정 (패턴 감지용)
KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
CLICK_STREAM_TOPIC = 'click_stream'
SUSPICIOUS_ACTIVITY_TOPIC = 'suspicious_activity'
SPARK_MASTER_URL = 'spark://spark-master:7077'

# Kafka 메시지 스키마 정의
CLICK_SCHEMA = StructType([
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("click_time", StringType(), True), # Initially String, will be converted to Timestamp
    StructField("referrer", StringType(), True),
    StructField("user_agent", StringType(), True)
])

def get_kafka_producer():
    # KafkaProducer는 직렬화 문제가 있을 수 있으므로, Executor에서 필요시 생성
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def main():
    spark = None
    try:
        spark = SparkSession.builder \
            .appName("ClickStreamAnalyzer") \
            .master(SPARK_MASTER_URL) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoints/click_analyzer") \
            .getOrCreate()
        
        # 로그 레벨 설정 (너무 많은 로그 방지)
        spark.sparkContext.setLogLevel("WARN")
        logging.info("SparkSession created successfully.")

        # 1. Kafka Source로부터 데이터 읽기
        raw_stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", CLICK_STREAM_TOPIC) \
            .option("startingOffsets", "latest") \
            .load()
        
        logging.info("Kafka source stream created.")

        # 2. JSON 데이터 파싱 및 Timestamp 변환
        parsed_stream_df = raw_stream_df \
            .selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), CLICK_SCHEMA).alias("data")) \
            .select("data.*")
        
        # click_time (String)을 event_timestamp (Timestamp)로 변환
        # ISO 8601 형식이므로 to_timestamp가 바로 변환 가능
        clicks_with_timestamp_df = parsed_stream_df \
            .withColumn("event_timestamp", to_timestamp(col("click_time")))
        
        logging.info("Data parsed and timestamp converted.")

        # --- 문제 5: Top 5 페이지 분석 로직 ---
        top_pages_df = clicks_with_timestamp_df \
            .withWatermark("event_timestamp", "1 minute") \
            .groupBy(
                window(col("event_timestamp"), "1 minute", "5 seconds"), # 1분 윈도우, 5초 슬라이드
                col("page_url")
            ) \
            .count() \
            .orderBy(desc("window"), desc("count")) \
            .limit(5)
        
        logging.info("Top 5 pages analysis logic defined.")
        
        # --- 문제 7: Top 3 사용자 분석 로직 (추가) ---
        top_users_df = clicks_with_timestamp_df \
            .withWatermark("event_timestamp", "1 minute") \
            .groupBy(
                window(col("event_timestamp"), "1 minute", "5 seconds"),
                col("user_id")
            ) \
            .count() \
            .orderBy(desc("window"), desc("count")) \
            .limit(3)
            
        logging.info("Top 3 users analysis logic defined.")

        # --- 문제 8: 의심스러운 활동 패턴 감지 로직 (추가) ---
        suspicious_activity_df = clicks_with_timestamp_df \
            .withWatermark("event_timestamp", "30 seconds") \
            .groupBy(
                window(col("event_timestamp"), "30 seconds", "10 seconds"), # 30초 윈도우, 10초 슬라이드
                col("user_id")
            ) \
            .agg(countDistinct("page_url").alias("distinct_page_count")) \
            .where(col("distinct_page_count") >= 5)
        
        logging.info("Suspicious activity detection logic defined.")

        # --- 결과 출력 (Console) ---
        # 문제 6: Top 5 페이지 결과 출력
        query_top_pages = top_pages_df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .queryName("TopPagesConsoleQuery") \
            .start()
        logging.info("Top pages console query started.")

        # 문제 7: Top 3 사용자 결과 출력
        query_top_users = top_users_df.writeStream \
            .outputMode("complete") \
            .format("console") \
            .option("truncate", False) \
            .queryName("TopUsersConsoleQuery") \
            .start()
        logging.info("Top users console query started.")
        
        # 문제 8 & 9: 의심스러운 활동 감지 결과를 Kafka로 전송 (콘솔 출력은 선택사항)
        query_suspicious_to_kafka = suspicious_activity_df.writeStream \
            .outputMode("update") \
            .foreachBatch(send_to_kafka_suspicious_activity) \
            .queryName("SuspiciousActivityToKafkaQuery") \
            .start()
        logging.info("Suspicious activity to Kafka query started.")
        
        # (선택사항) 의심 활동 감지 결과를 콘솔에도 간략히 출력
        # query_suspicious_to_console = suspicious_activity_df.writeStream \
        #     .outputMode("update") \
        #     .format("console") \
        #     .option("truncate", False) \
        #     .queryName("SuspiciousActivityConsoleQuery") \
        #     .start()
        # logging.info("Suspicious activity console query started.")

        # 모든 쿼리가 종료될 때까지 대기
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logging.error(f"An error occurred in Spark application: {e}", exc_info=True)
    finally:
        if spark:
            logging.info("Stopping SparkSession...")
            spark.stop()
            logging.info("SparkSession stopped.")

def send_to_kafka_suspicious_activity(batch_df, epoch_id):
    if batch_df.count() > 0:
        logging.info(f"Epoch {epoch_id}: Detected suspicious activities. Sending to Kafka topic '{SUSPICIOUS_ACTIVITY_TOPIC}'.")
        producer = get_kafka_producer()
        for row in batch_df.collect():
            message = row.asDict()
            # window 객체를 문자열로 변환 (Kafka 전송을 위해)
            if 'window' in message and hasattr(message['window'], 'start') and hasattr(message['window'], 'end'):
                message['window_start'] = message['window'].start.isoformat()
                message['window_end'] = message['window'].end.isoformat()
                del message['window'] # 원본 window 객체 삭제
            
            producer.send(SUSPICIOUS_ACTIVITY_TOPIC, value=message)
            logging.info(f"Sent to Kafka: {message}")
        producer.flush()
        producer.close()
    else:
        logging.info(f"Epoch {epoch_id}: No suspicious activities detected in this batch.")

if __name__ == '__main__':
    main() 