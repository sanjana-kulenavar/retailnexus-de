# 🏪 RetailNexus DE — Cloud-Native Retail Analytics Platform

[![CI Pipeline](https://github.com/sanjana-kulenavar/retailnexus-de/actions/workflows/ci.yml/badge.svg)](https://github.com/sanjana-kulenavar/retailnexus-de/actions)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.8-orange.svg)](https://getdbt.com)
[![Apache Kafka](https://img.shields.io/badge/kafka-3.7-black.svg)](https://kafka.apache.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## What Is This?

RetailNexus DE is a production-grade, end-to-end retail analytics data platform
built to demonstrate modern Data Engineering practices on a cloud-native stack.

It processes real-time Point-of-Sale (POS) transaction streams and batch retail
data across three business domains — **Sales Audit**, **Price Management**, and
**Inventory Merchandising** — mirroring the same business logic used in enterprise
Oracle Retail systems (ReSA, RPM, RMS), rebuilt on the modern data stack.

> 💡 **Domain context:** This project translates 8+ years of Oracle Retail
> implementation experience into a cloud-native architecture. The Sales Audit
> pipeline replicates Oracle ReSA RTLOG processing. The Price Management pipeline
> implements the same SCD Type 2 history that Oracle RPM maintains. The Inventory
> pipeline mirrors Oracle RMS stock-level reconciliation — rebuilt with open-source,
> cloud-native tools.

---

## Architecture

[POS Simulator] ──► [Apache Kafka] ──► [PySpark Consumer]
│
[S3 Bronze Layer]
(Raw Parquet, date-partitioned)
│
[PySpark Transform Jobs]
│
[S3 Silver Layer]
(Delta Lake tables)
│
[Great Expectations Validation]
│
[dbt Core Models]
(Staging → Marts in Snowflake)
│
[Snowflake Gold Layer]
(Fact & Dimension tables)
│
[Apache Superset Dashboards]

Orchestration: Apache Airflow
Infrastructure: Terraform (AWS S3 + Snowflake)
CI/CD: GitHub Actions (dbt test gates on every PR)
Local Dev: Docker Compose (Kafka, Airflow, Superset)


---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Streaming Ingestion | Apache Kafka | 3.7 |
| Batch Ingestion | Python | 3.11 |
| Data Lake Storage | AWS S3 | — |
| Table Format | Delta Lake | 3.x |
| Distributed Processing | Apache Spark (PySpark) | 3.5 |
| SQL Transformation | dbt Core | 1.8 |
| Data Warehouse | Snowflake | — |
| Orchestration | Apache Airflow | 2.9 |
| Data Quality | Great Expectations | 0.18 |
| Visualisation | Apache Superset | 3.x |
| Infrastructure as Code | Terraform | 1.7 |
| Containers | Docker + Compose | 24+ |
| CI/CD | GitHub Actions | — |

### Cloud Equivalents
This project runs on AWS. The architecture is cloud-agnostic:
- **AWS S3** → Azure ADLS Gen2 → GCP Cloud Storage
- **PySpark (local)** → Azure Databricks → GCP Dataproc
- **Snowflake** → Azure Synapse Analytics → GCP BigQuery
- **Apache Airflow** → Azure Data Factory → GCP Cloud Composer
- **Terraform** provisions infrastructure on any of the three clouds

---

## Project Status

| Domain | Pipeline | Status |
|---|---|---|
| Sales Audit (ReSA) | Kafka → Bronze → Silver → Gold | 🔨 In Progress |
| Price Management (RPM) | Batch → Bronze → Silver → Gold | 📋 Planned |
| Inventory (RMS) | Batch → Bronze → Silver → Gold | 📋 Planned |

---

## Getting Started

### Prerequisites
- Windows 10/11 with WSL2 (Ubuntu 22.04)
- Docker Desktop 24+ with WSL2 backend enabled
- Python 3.11 inside WSL2
- Git

### 1. Clone the Repository
```bash
git clone git@github.com:YOURUSERNAME/retailnexus-de.git
cd retailnexus-de
```

### 2. Set Up Python Environment
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables
```bash
cp .env.example .env
# Edit .env with your AWS and Snowflake credentials
```

### 4. Start Local Infrastructure
```bash
docker compose up -d
```

Services started:
| Service | URL | Credentials |
|---|---|---|
| Kafka UI | http://localhost:8080 | — |
| Airflow UI | http://localhost:8081 | admin / admin |

### 5. Verify Everything is Running
```bash
docker compose ps
# All services should show "healthy" or "running"
```

---

## Repository Structure

retailnexus-de/
├── ingestion/
│   ├── kafka_producer/      # POS transaction simulator
│   └── kafka_consumer/      # PySpark Kafka → S3 Bronze
├── processing/
│   └── spark_jobs/          # PySpark Bronze → Silver transforms
├── dbt_project/             # dbt: staging → marts in Snowflake
├── airflow/dags/            # Airflow DAGs (one per domain)
├── data_quality/            # Great Expectations suites
├── terraform/               # AWS + Snowflake IaC
├── dashboards/              # Superset exports
├── docs/                    # Architecture diagrams
└── .github/workflows/       # CI/CD pipelines


---

## Author

**Sanjana P Kulenavar**  
Senior Consultant → Data Engineer  
8+ years Oracle Retail (ReSA, RPM, RMS, RIB)  
[LinkedIn](https://linkedin.com/in/sanjana-kulenavar-21a78621b/) | [GitHub](https://github.com/sanjana-kulenavar)
