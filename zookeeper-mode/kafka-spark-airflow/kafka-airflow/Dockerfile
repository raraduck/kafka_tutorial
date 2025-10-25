FROM apache/airflow:2.8.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    apache-airflow-providers-apache-kafka==1.6.0 \
    apache-airflow-providers-apache-spark==4.3.0 \
    psycopg2-binary==2.9.9 
