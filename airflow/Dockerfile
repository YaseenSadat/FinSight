FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
USER airflow

RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    pyarrow \
    boto3 \
    s3fs \
    "apache-airflow-providers-amazon==8.16.0" \
    "apache-airflow-providers-snowflake==5.5.2" \
    snowflake-connector-python \
    "pyspark==3.5.1"
