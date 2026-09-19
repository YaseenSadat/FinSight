# Airflow image with FinSight installed (plus the S3, Spark, and Snowflake extras).
#
# Built by docker-compose.yml from the repository root:
#   docker compose build
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
# Must match the Spark version of the cluster in docker-compose.yml.
ARG PYSPARK_VERSION=4.1.3

USER root
# Java runtime for the Spark driver, which runs inside Airflow tasks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY --chown=airflow:root pyproject.toml README.md LICENSE /opt/finsight/
COPY --chown=airflow:root src /opt/finsight/src
# Pin Airflow so installing FinSight can never up- or downgrade it.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "/opt/finsight[s3,snowflake]" \
    "pyspark==${PYSPARK_VERSION}"
