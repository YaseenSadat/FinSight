Here is a tightened, polished README for **FinSight** with the sections you requested removed and with emphasis on automated DAGs, Airflow, and Snowflake.

---

# **FinSight — End to End Stock Data ELT Pipeline**

FinSight is a compact but powerful ELT platform for financial market data. It ingests end of day stock prices, transforms them using distributed compute, validates quality, and publishes analytics ready tables into Snowflake. The architecture mirrors modern cloud data engineering patterns while running locally in a fully dockerized stack.

---

## **Overview**

FinSight follows a clean tiered design that reflects real production pipelines:

* **Bronze**: raw ingestion into an S3 compatible object store
* **Silver**: curated columnar datasets produced with Spark
* **Gold**: validated data loaded into Snowflake for querying and analytics

Each step is automated by Apache Airflow. Workflows are parameterized and can run on demand or on a schedule.

---

## **Automated Airflow Workflows**

FinSight uses small, focused DAGs in Apache Airflow, with all processing logic stored in reusable modules. This keeps orchestration readable while preserving strong separation of concerns.

**Main DAGs:**

### **1. Bronze Ingestion**

Pulls end of day prices from a financial API and writes raw parquet files.
Includes manifest generation and clear partitioning by symbol and load date.

### **2. Silver Transformation**

Runs a distributed Spark job that standardizes the raw feed, enforces schema consistency, and produces curated datasets.

### **3. Gold Loading and Validation**

Validates curated partitions and loads them into a Snowflake table:

**`STOCKS.CURATED.EOD_PRICES`**

This ensures only complete and correct data reaches the warehouse.

---

## **Cloud Style Local Stack**

FinSight runs on a dockerized environment that models a real data platform:

* **Apache Airflow 2.9** for orchestration and automation
* **PySpark / Spark 3.5** for distributed transformation
* **MinIO** as an S3 compatible bronze and silver layer
* **Postgres** for Airflow metadata
* **Snowflake connector** for publishing gold datasets

This gives a realistic workflow without requiring any cloud services.

---

## **How FinSight Works**

1. **Ingest**
   Collects market data, writes bronze parquet, and registers a manifest.

2. **Transform**
   Spark curates the raw dataset into a consistent, analytics friendly schema.

3. **Validate**
   Ensures completeness, column integrity, and expected ranges.

4. **Load**
   Writes the final partition into the Snowflake gold table for downstream use.

---

## **Project Highlights**

* Automated DAGs with Airflow for ingestion, transformation, and loading
* Spark powered processing for scalable data shaping
* S3 style storage through MinIO with clear tiering
* Gold table stored in Snowflake for fast analytics
* Modular code structure for clean orchestration
* Easy to demo and extend, ideal for showcasing data engineering ability

---

If you want, I can also make a shorter GitHub friendly version or design a simple architecture diagram for the README.
