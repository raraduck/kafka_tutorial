from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def print_hello():
    """
    A simple Python function that prints "Hello World!".
    """
    print("Hello World!")

# 5-minute schedule
with DAG(
    dag_id="kafka_batch_to_postgres_every_5min",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["kafka","spark","postgres","batch"],
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello_task',
        python_callable=print_hello,
    )

    # run_spark_job = SparkSubmitOperator(
    #     task_id="submit_spark_test_job",
    #     conn_id="spark_default",
    #     application="/opt/spark-apps/consume_kafka_to_postgres_batch.py",
    #     verbose=True,
    #     conf={
    #         "spark.master": "spark://spark-master:7077",
    #         "spark.submit.deployMode": "client",
    #     },
    #     name="spark_test_job",
    #     executor_memory="1g",
    #     driver_memory="1g",
    #     dag=None,  # DAG에 등록하지 않음
    # )

    # hello_task


    # 1) Submit Spark batch via SparkSubmitOperator
    # spark_submit_op = SparkSubmitOperator(
    #     task_id="spark_submit_operator_run",
    #     conn_id=None,  # using direct master arg
    #     application="/opt/spark-apps/consume_kafka_to_postgres_batch.py",
    #     verbose=True,

    #     name="spark-submit-operator-batch",

    #     # java_class=None,
    #     # application_args=[],
    #     # jars=None,
    #     # packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.7.3",
    # )

    # # 1) Submit Spark batch via SparkSubmitOperator
    # spark_submit_op = SparkSubmitOperator(
    #     task_id="spark_submit_operator_run",
    #     application="/opt/spark-apps/batch_job.py",
    #     name="spark-submit-operator-batch",
    #     conn_id=None,  # using direct master arg
    #     java_class=None,
    #     application_args=[],
    #     jars=None,
    #     packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.7.3",
    #     driver_class_path=None,
    #     driver_memory="1g",
    #     executor_memory="1g",
    #     num_executors=1,
    #     conf={"spark.master": "spark://spark-master:7077"},
    #     env_vars={
    #         "KAFKA_BROKERS": os.environ.get("KAFKA_BROKERS","kafka:29092"),
    #         "KAFKA_TOPIC": os.environ.get("KAFKA_TOPIC","user_events"),
    #         "PG_HOST": os.environ.get("PG_HOST","postgres"),
    #         "PG_PORT": os.environ.get("PG_PORT","5432"),
    #         "PG_DB": os.environ.get("PG_DB","airflow"),
    #         "PG_USER": os.environ.get("PG_USER","airflow"),
    #         "PG_PASSWORD": os.environ.get("PG_PASSWORD","airflow"),
    #     },
    #     verbose=True,
    # )

    # # 2) Submit Spark batch via BashOperator (spark-submit CLI) — second submit method
    # spark_submit_cli = BashOperator(
    #     task_id="spark_submit_cli_run",
    #     bash_command=(
    #         "spark-submit "
    #         "--master spark://spark-master:7077 "
    #         "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.7.3 "
    #         "/opt/spark-apps/batch_job.py"
    #     ),
    #     env={
    #         "KAFKA_BROKERS": os.environ.get("KAFKA_BROKERS","kafka:29092"),
    #         "KAFKA_TOPIC": os.environ.get("KAFKA_TOPIC","user_events"),
    #         "PG_HOST": os.environ.get("PG_HOST","postgres"),
    #         "PG_PORT": os.environ.get("PG_PORT","5432"),
    #         "PG_DB": os.environ.get("PG_DB","airflow"),
    #         "PG_USER": os.environ.get("PG_USER","airflow"),
    #         "PG_PASSWORD": os.environ.get("PG_PASSWORD","airflow"),
    #     }
    # )

    # # 3) Postgres aggregation (upsert minute-level counts)
    # aggregate_sql = PostgresOperator(
    #     task_id="aggregate_minute_counts",
    #     postgres_conn_id="postgres_default",
    #     database="airflow",
    #     sql=\"\"\"
    #     WITH agg AS (
    #       SELECT date_trunc('minute', event_time) AS bucket,
    #              action,
    #              count(*) AS cnt
    #       FROM analytics.user_events_stream
    #       WHERE event_time > now() - interval '1 day'
    #       GROUP BY 1,2
    #     )
    #     SELECT analytics.upsert_event_counts_minute(bucket, action, cnt)
    #     FROM agg;
    #     \"\"\"
    # )

    # Define order: run one of the spark submits first, then the other, then aggregate
    # You can switch order if you prefer; they are independent demos of submission styles.
    # spark_submit_op >> spark_submit_cli >> aggregate_sql
