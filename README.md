# Real-Time E-Commerce Clickstream Analytics Pipeline

Real-time data pipeline using Kafka, Spark Structured Streaming, and Airflow to process e-commerce clickstream data with Bronze-Silver-Gold architecture.

## 🏗️ Architecture

**Bronze Layer** → Raw events in Kafka ✅  
**Silver Layer** → Cleaned Parquet files with Spark ✅  
**Gold Layer** → Aggregated metrics (Planned)

```
Producer (5 events/sec) → Kafka (3 partitions) → Spark Streaming → Parquet Files
                           [Bronze Layer]         [Transformation]   [Silver Layer]
```



## 🚀 Quick Start

### Setup & Run

```cmd
# 1. Start services
docker-compose up -d

# 2. Create Kafka topic
docker exec -it kafka kafka-topics --create --topic ecom_clickstream_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# 3. Start producer (Terminal 1)
python ecom_producer.py

# 4. Start Spark job (Terminal 2)
submit_spark_job.bat
```

### Verify

```cmd
# Check Spark Master
http://localhost:8080

# Check Application UI
http://localhost:4040

# View Silver layer files
docker exec spark-master ls -R /tmp/silver_layer/
```



## 📊 Data Flow

**Producer** (`ecom_producer.py`) → **Kafka** (`ecom_clickstream_raw`) → **Spark Streaming** → **Parquet Files** (`/tmp/silver_layer/`)

### Transformations
- Parse JSON from Kafka
- Filter nulls and invalid prices
- Deduplicate by event_id
- Add `is_high_value` flag (price > $500)
- Extract `event_date` and `event_hour`
- Partition by date and event_type

## 🌐 Web Interfaces

| Service | URL | Description |
|---------|-----|-------------|
| Spark Master | http://localhost:8080 | Cluster status & applications |
| Spark App | http://localhost:4040 | Streaming metrics (when job running) |
| Airflow | http://localhost:8087 | Workflow UI (planned) |

## 📂 Project Structure

```
Final Project/
├── docker-compose.yml          # 8 services: Zookeeper, Kafka, PostgreSQL, Airflow (3), Spark (2)
├── ecom_producer.py            # Kafka producer (5 events/sec, 60s)
├── spark_streaming_silver.py   # Spark job (Bronze → Silver)
├── submit_spark_job.bat        # Windows script to submit Spark job
├── verify_silver_layer.py      # Verification script
└── README.md
```

## 🛠️ Technologies

| Technology | Version | Purpose |
|------------|---------|---------|
| Apache Kafka | 7.4.1 | Event streaming |
| Apache Spark | 3.5.0 | Stream processing |
| Apache Airflow | 2.8.3 | Orchestration (planned) |
| Docker | Latest | Containerization |
| Python | 3.8+ | Producer & scripts |

## 🔧 Configuration

### Kafka
- **Topic**: `ecom_clickstream_raw`
- **Partitions**: 3
- **Bootstrap**: `localhost:9092`
- **Format**: JSON

### Spark Streaming
- **Trigger Interval**: 20 seconds
- **Checkpoint**: `/tmp/spark-checkpoints/silver`
- **Output Format**: Parquet (Snappy)
- **Partitioning**: `event_date`, `event_type`

## 🚨 Troubleshooting

**Kafka connection failed?**
```cmd
docker-compose restart kafka
docker exec -it kafka kafka-broker-api-versions --bootstrap-server localhost:9092
```

**Spark UI (4040) not accessible?**
- Only available when job is running
- Check Spark Master UI (8080) for application status

**No data in Silver layer?**
```cmd
docker exec -it kafka kafka-console-consumer --topic ecom_clickstream_raw --bootstrap-server localhost:9092 --max-messages 5
docker logs spark-master
```

## 🔮 Next Steps

### Phase 4: Gold Layer (Planned)
- Conversion rate analytics
- High-interest, low-conversion detection
- Time-windowed aggregations

### Phase 5: Airflow DAGs (Planned)
- Automated workflows
- Scheduling & monitoring
- Alerting