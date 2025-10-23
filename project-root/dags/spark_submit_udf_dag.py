from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="spark_submit_udf_dag",
    default_args=default_args,
    description="Simple SparkSubmitOperator test DAG",
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=["spark", "test"],
) as dag:

    run_spark_job = SparkSubmitOperator(
        task_id="submit_spark_udf_job",
        # conn_id="spark_default",
        conn_id="spark_local",
        application="/opt/spark-apps/spark_udf_job.py",
        verbose=True,
        conf={
            # "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client",
        },
        name="spark_udf_job",
        executor_memory="1g",
        driver_memory="1g",
    )

    run_spark_job
