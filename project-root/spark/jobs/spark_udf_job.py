from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Spark 세션 생성
spark = SparkSession.builder.appName("SparkTestJob").getOrCreate()

# 샘플 데이터
data = [("Alice", 30), ("Bob", 25), ("Charlie", 28)]
df = spark.createDataFrame(data, ["name", "age"])

print("\n[1] Original Data:")
df.show()

# ✅ (1) UDF 정의 — 이름의 첫 글자를 대문자로 추출
def get_initial(name: str) -> str:
    if name and len(name) > 0:
        return name[0].upper()
    return None

# ✅ (2) Spark UDF 등록
get_initial_udf = udf(get_initial, StringType())

# ✅ (3) UDF를 이용해 새 컬럼 추가
df_with_initial = df.withColumn("initial", get_initial_udf(col("name")))

print("\n[2] Data with Initial:")
df_with_initial.show()

# ✅ (4) 나이 필터링
df_filtered = df_with_initial.filter(col("age") > 26)

print("\n[3] Filtered Data (age > 26):")
df_filtered.show()

# ✅ (5) CSV로 저장
output_path = "/tmp/spark_output"
df_filtered.coalesce(1).write.mode("overwrite").csv(output_path, header=True)

print("\n✅ Spark job completed successfully. Output saved to:", output_path)

# 세션 종료
spark.stop()

