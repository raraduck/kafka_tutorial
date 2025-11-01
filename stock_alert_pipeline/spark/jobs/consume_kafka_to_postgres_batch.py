from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StringType, IntegerType

# Kafka JSON 스키마 정의
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("event", StringType()) \
    .add("timestamp", StringType())

# Spark 세션
spark = SparkSession.builder \
    .appName("KafkaToPostgresBatch") \
    .getOrCreate()

# Kafka에서 데이터 배치로 읽기 (처음부터 전체)
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-zk-broker1:9092") \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "earliest") \
    .load()

# Kafka 메시지(JSON)를 파싱
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")

# PostgreSQL에 저장 (기존 내용 덮어쓰기)
json_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/airflow") \
    .option("dbtable", "user_events_stream") \
    .option("user", "airflow") \
    .option("password", "airflow") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

spark.stop()