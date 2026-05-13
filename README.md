#  Crypto Data Warehouse Pipeline
## Project Overview

This project builds an **end-to-end Data Engineering pipeline** to collect, process, and analyze cryptocurrency market and news data.

The system extracts data from external APIs, processes it using **Apache Spark**, and loads analytical datasets into a **PostgreSQL Data Warehouse** designed with a **Star Schema**.

The architecture follows the modern **Medallion Architecture**:

```
Raw → Staging → Curated → Data Warehouse (Dim / Fact)
```

---

## Data Pipeline Architecture

<div align="center">
  <img src="flow-data.png" style="width:100%;"/>
</div>

---

## Data Layers

### 1️⃣ Raw Layer

* Stores original data from APIs
* Format: `JSON / JSONL`
* Immutable (no modification)
* Used for replay & debugging

**Example:**

```
data/raw/market/
data/raw/news/
```

---

### 2️⃣ Staging Layer

Initial data cleaning and standardization.

**Processes:**

* JSON parsing
* Data type casting
* Remove null values
* Standardize schema

**Output:**

* Parquet format (optimized for analytics)

---

### 3️⃣ Curated Layer

Business-ready datasets used for analytics and modeling.

**Processes:**

* Data validation & deduplication
* Date standardization
* Data enrichment (coin mapping, normalization)
* Partitioning for performance

**Example:**

* Daily crypto price data
* News data mapped to coins

---

### 4️⃣ Data Warehouse Layer

Implements a **Star Schema** for analytical queries.

#### Dimension Tables

* `dim_coin`
* `dim_date`
* `dim_source`

####  Fact Tables

* `fact_market` → crypto prices & metrics
* `fact_news` → aggregated news per coin/day/source

---

##  Data Warehouse Schema

```
dim_coin ─────┐
              ├── fact_market ─── dim_date

dim_coin ─────┐
dim_source ───┼── fact_news
              │
              └── dim_date
```

---

##  Run with Docker

Start all services:

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

---

##  Run Pipeline Manually

### Step 1 — Data Ingestion (Extract)

```bash
python crawl/crawl_market.py
python crawl/crawl_news.py
```

---

### Step 2 — Transform Raw → Staging

```bash
spark-submit spark_jobs/transform_raw.py
```

---

### Step 3 —  Data Warehouse + Load into PostgreSQL

```bash
spark-submit spark_jobs/build_dim.py
spark-submit spark_jobs/build_fact.py
spark-submit spark_jobs/incremental_load

```

---

##  PostgreSQL Configuration

| Property | Value          |
| -------- | -------------- |
| Database | warehouse      |
| Schema   | warehouse_coin |
| User     | dw             |
| Password | dw             |
| Port     | 5432           |

---

##  Key Features

* Distributed data processing with **Apache Spark**
* Medallion Architecture (Raw → Curated)
* Star Schema Data Warehouse
* Incremental ETL pipeline
* Parquet-based Data
* Dockerized environment
* Ready for orchestration (Airflow)

---

## Future Improvements

* Airflow DAG orchestration
* Incremental CDC (Change Data Capture)
* Data Quality checks (Great Expectations)
* Dashboard visualization (Power BI / Superset)
* Real-time streaming (Kafka + Spark Streaming)

---

##  Author

**Huu Tam Nguyen**
Data Engineering Project
