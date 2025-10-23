from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="spark_submit_test_dag",
    default_args=default_args,
    description="Simple SparkSubmitOperator test DAG",
    # schedule_interval="*/5 * * * *",  
    schedule_interval=None,  # ✅ 수동 실행 전용
    catchup=False,  
    tags=["spark", "test"],
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id="submit_spark_test_job",
        # master="local[*]",   # ✅ SparkSubmitOperator 인자에 직접 지정
        conn_id="spark_local",
        # conn_id="spark_default",
        application="/opt/spark-apps/consume_kafka_to_postgres_batch.py",
        verbose=True,
        conf={
            # "spark.master": "local[*]",             # ✅ local 모드로 변경
            # "spark.master": "spark://spark-master:7077",
            # "spark.driver.host": "airflow-webserver",   # ✅ Spark 드라이버의 실제 컨테이너 호스트명
            "spark.submit.deployMode": "client", # "cluster", # "client",
            "spark.driver.bindAddress": "0.0.0.0",        # ✅ 추가
            "spark.shuffle.push.enabled": "false",   # ✅ 중요
            "spark.executor.cores": "1",        # ✅ 추가: executor 1개 코어만
            "spark.executor.instances": "1",    # ✅ 추가: executor 1개만 띄우기
        },
        # master="local[*]",    # ✅ SparkSubmitOperator의 명시적 인자 사용
        name="spark_test_job",
        # executor_memory="1g",
        # driver_memory="1g",
        executor_memory="512m",   # ✅ 절반 줄이기
        driver_memory="512m",     # ✅ 절반 줄이기
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0"
    )

    run_spark_job
