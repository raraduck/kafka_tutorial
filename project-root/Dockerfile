FROM apache/airflow:2.8.1

USER root
# -------------------------------------------------------
# 1️⃣ OS 패키지 업데이트 및 OpenJDK 17 설치
# -------------------------------------------------------
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    build-essential \
    librdkafka-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
# -------------------------------------------------------
# 3️⃣ JAVA_HOME 환경변수 설정
# -------------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow
RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    apache-airflow-providers-apache-kafka==1.6.0 \
    apache-airflow-providers-apache-spark==4.3.0 \
    psycopg2-binary==2.9.9 
