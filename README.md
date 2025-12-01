# Real-Time E-Commerce Clickstream Analytics Pipeline

A comprehensive big data pipeline implementing **Kappa Architecture** for real-time stream processing and **Apache Airflow** for batch analytics orchestration, processing e-commerce clickstream events to generate flash sale alerts and daily analytical reports.

## Architecture Overview

### Real-Time Layer (Kappa Architecture)
```
Producer → Kafka → Spark Streaming → PostgreSQL
(Events)   (Buffer)  (Aggregation)    (Alerts)
```

### Batch Analytics Layer
```
Airflow DAGs → Scheduled Analytics → Reports
(Orchestration)  (Daily Processing)   (Insights)
```

## Features

### Real-Time Processing
- **Flash Sale Detection**: Identifies high-view, low-conversion products (>100 views, <5 purchases) for immediate marketing action using 10-minute sliding windows

### Batch Analytics (Airflow DAGs)
- **User Segmentation (daily_user_segmentation)**: Categorizes users into Window Shoppers vs Buyers, scheduled daily at 1:00 AM
- **Top Products Report (top_products_daily_report)**: Ranks top 5 most-viewed products with engagement metrics, scheduled daily at 2:00 AM
- **Conversion Rate Analytics (conversion_rate_analytics)**: Calculates category-level purchase conversion rates and identifies best/worst performers, scheduled daily at 3:00 AM

### Orchestration
- **Automated Scheduling**: All DAGs run automatically via Airflow scheduler
- **Task Dependencies**: Extract → Transform → Save + Report generation in parallel
- **Error Handling**: Retry logic and failure notifications

## Quick Start

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Create Kafka Topic
```bash
docker exec -it kafka kafka-topics --create --topic ecom_clickstream_data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 3. Start Real-Time Pipeline
```bash
# Terminal 1: Producer
python ecom_producer.py

# Terminal 2: Spark Streaming
scripts\submit_spark_kappa.bat
```

### 4. Trigger Batch Analytics
```bash
scripts\trigger_airflow_dags.bat
```

### 5. Verify Status
```bash
scripts\check_dags_status.bat
```

## Project Structure

```
Final Project/
├── docker-compose.yml          # Infrastructure: Kafka, Spark, Airflow, PostgreSQL
├── ecom_producer.py            # Kafka event producer (5 events/sec)
├── spark_streaming_kappa.py    # Real-time stream processing job
├── dags/                       # Airflow DAG definitions
│   ├── daily_user_segmentation_dag.py
│   ├── top_products_daily_dag.py
│   └── conversion_rate_analytics_dag.py
├── scripts/                    # Automation scripts
│   ├── submit_spark_kappa.bat
│   ├── trigger_airflow_dags.bat
│   ├── verify_analytics.bat
│   └── check_dags_status.bat
└── docs/                       # Documentation
    ├── ORCHESTRATION_GUIDE.md
    ├── COMPLETE_RUN_SEQUENCE.md
    └── RUN_GUIDE.md
```

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Stream Processing | Apache Spark | 3.5.0 | Real-time aggregations |
| Message Broker | Apache Kafka | 7.4.1 | Event streaming |
| Orchestration | Apache Airflow | 2.8.3 | Batch job scheduling |
| Database | PostgreSQL | 13 | Data persistence |
| Containerization | Docker Compose | - | Service deployment |

## Key Metrics

### Real-Time Alerts
- **Trigger Condition**: Views > 100 AND Purchases < 5
- **Window**: 10-minute sliding windows (1-minute slide)
- **Output**: `flash_sale_alerts` table

### Batch Analytics DAGs (Daily Schedule)

| DAG | Schedule | Tasks | Output |
|-----|----------|-------|--------|
| `daily_user_segmentation` | 1:00 AM | Extract events → Segment users → Save + Email report | User categories with purchase behavior |
| `top_products_daily_report` | 2:00 AM | Extract views → Calculate rankings → Save + Format report | Top 5 products with engagement metrics |
| `conversion_rate_analytics` | 3:00 AM | Extract all events → Calculate rates → Save + Analysis report | Category conversion rates & trends |

## Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8087 | admin / admin |
| Spark Master | http://localhost:8080 | - |
| Spark Application | http://localhost:4040 | - |

## Verification

### Check Real-Time Alerts
```bash
docker exec -it postgres psql -U airflow -d airflow -c "SELECT * FROM flash_sale_alerts ORDER BY alert_timestamp DESC LIMIT 10;"
```

### View Batch Reports
1. Open Airflow UI (http://localhost:8087)
2. Click on DAG → Latest Run
3. Click task → Logs to view reports

### System Health Check
```bash
scripts\check_dags_status.bat
```

## Documentation

- **`docs/ORCHESTRATION_GUIDE.md`**: Complete Airflow implementation guide
- **`docs/COMPLETE_RUN_SEQUENCE.md`**: Step-by-step execution workflow
- **`docs/RUN_GUIDE.md`**: Detailed operational procedures

## Requirements

- Docker Desktop (Windows)
- Python 3.8+
- 8GB RAM minimum
- Ports: 8080, 8087, 9092, 5432, 4040

## Author

Applied Big Data Engineering Mini Project  
Scenario 3: E-Commerce Clickstream Analytics
