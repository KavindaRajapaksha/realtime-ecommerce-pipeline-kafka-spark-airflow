# Real-Time E-Commerce Clickstream Analytics Pipeline

A real-time data engineering project that implements a Lambda Architecture using Kafka, Spark Structured Streaming, and Apache Airflow for processing e-commerce clickstream data.

## Architecture Overview

This project implements a Bronze-Silver-Gold (Medallion) data pipeline architecture:

-   **Bronze Layer**: Contains raw, immutable event data ingested from Kafka.
-   **Silver Layer**: Features cleaned, validated, and deduplicated data (upcoming).
-   **Gold Layer**: Consists of aggregated business metrics and analytics (upcoming).

## Implementation Status

### Phase 1: Infrastructure Setup

**Description:**
A complete Docker-based data engineering infrastructure was established, configuring multiple services to operate within a containerized environment.

**Rationale:**
This setup provides isolated and reproducible environments for all services, enables straightforward deployment and scaling, and ensures consistency across different development environments.

**Deployed Services:**
1.  **Zookeeper** (Port 2181): A coordination service for Kafka.
2.  **Kafka** (Ports 9092, 29092): A distributed event streaming platform.
3.  **PostgreSQL** (Port 5432): The metadata database for Airflow.
4.  **Apache Airflow** (Port 8087): A workflow orchestration tool, comprising:
    -   Webserver: A user interface for managing workflows.
    -   Scheduler: Executes DAGs according to their schedules.
    -   Init: Manages database initialization and user creation.
5.  **Apache Spark** (Ports 8080, 7077): A distributed data processing engine, including:
    -   Master: The cluster manager.
    -   Worker: A processing node.

### Phase 2: Data Ingestion (Bronze Layer)

**Description:**
A Kafka topic named `ecom_clickstream_raw` was created with three partitions. A Python-based data producer, `ecom_producer.py`, was developed with logic to generate events that simulate realistic e-commerce clickstream patterns.

**Rationale:**
This phase establishes the foundation for streaming data processing and creates an immutable event log, which is a core principle of the Bronze layer. It also simulates real-world clickstream behavior for testing purposes.

**Producer Key Features:**
-   Generates 500 unique users and 100 products.
-   Simulates three event types: `view`, `add_to_cart`, and `purchase`.
-   Implements a high-interest product (`prod_0042`) that receives 20% of all events.
-   The high-interest product has a low conversion rate (95% views, 4% add to cart, 1% purchase).
-   Partitions events by `user_id` to ensure ordered processing for each user.
-   Includes comprehensive event metadata, such as timestamp, session_id, device_type, and price.

## Quick Start Guide

### Prerequisites

-   Docker Desktop installed and running.
-   Python 3.8+ installed.
-   Git for version control.

### Installation and Execution

#### 1. Clone and Navigate to the Project Directory

```sh
cd "E:\8- Sem\Big Data\Final Project"
```

#### 2. Start Docker Services

Start all services defined in `docker-compose.yml` by running them in detached mode:
```sh
docker-compose up -d
```

#### 3. Verify Service Status

Confirm that all services are running:
```sh
docker-compose ps
```
The expected output should show all services with a status of "Up" or "running".

#### 4. Wait for Service Initialization

Please allow approximately 2-3 minutes for all services, particularly Airflow, to initialize completely.

#### 5. Create the Kafka Topic

Create the `ecom_clickstream_raw` Kafka topic with 3 partitions and a replication factor of 1 (suitable for development):
```sh
docker exec -it kafka kafka-topics --create --topic ecom_clickstream_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### 6. Verify Topic Creation

List the available Kafka topics to confirm creation:
```sh
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```
The output should include `ecom_clickstream_raw`.

#### 7. View Topic Details

Describe the topic to view its configuration details:
```sh
docker exec -it kafka kafka-topics --describe --topic ecom_clickstream_raw --bootstrap-server localhost:9092
```

#### 8. Install Python Dependencies

Install the required Kafka Python client library:
```sh
pip install kafka-python
```

#### 9. Run the Data Producer

Execute the producer script to generate and publish events to the Kafka topic. The script will run for 60 seconds and produce 5 events per second, for a total of 300 events.
```sh
python ecom_producer.py
```
The console will display real-time output showing the partition and offset for each sent message.
```
Kafka Producer connected to ['localhost:9092']

Starting event production...
============================================================
Sent: view            | User: user_0123   | Product: prod_0042   | Partition: 1 | Offset: 0
Sent: add_to_cart     | User: user_0456   | Product: prod_0033   | Partition: 2 | Offset: 0
...
Producer closed. Total events sent: 300
```

#### 10. Monitor the Kafka Topic (Optional)

In a new terminal, you can consume and display the first 10 messages from the topic to verify that data is being ingested correctly:
```sh
docker exec -it kafka kafka-console-consumer --topic ecom_clickstream_raw --from-beginning --bootstrap-server localhost:9092 --max-messages 10
```

#### 11. Check Message Count

Verify that all events were successfully published by checking the offset (message count) for each partition:
```sh
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic ecom_clickstream_raw
```

## Web Interfaces

-   **Apache Airflow UI**: `http://localhost:8087`
    -   **Username**: `admin`
    -   **Password**: `admin`
-   **Spark Master UI**: `http://localhost:8080`

## Data Schema

Each event in the Bronze layer adheres to the following JSON schema:

```json
{
  "event_id": "evt_00000001",
  "user_id": "user_0042",
  "product_id": "prod_0042",
  "event_type": "view",
  "timestamp": "2025-11-29T12:34:56.789Z",
  "session_id": "session_5678",
  "product_category": "Electronics",
  "device_type": "mobile",
  "page_url": "/product/prod_0042",
  "price": 299.99
}
```

## Configuration Details

### Kafka Configuration
-   **Bootstrap Servers**: `localhost:9092`
-   **Topic**: `ecom_clickstream_raw`
-   **Partitions**: 3
-   **Replication Factor**: 1
-   **Partition Key**: `user_id` (ensures ordered processing per user)

### Producer Settings
-   **Events per second**: 5 (configurable)
-   **Duration**: 60 seconds (configurable)
-   **Acknowledgment (`acks`)**: `all` (ensures durability)
-   **Retries**: 3
-   **Serialization**: JSON

### High-Interest Product Logic
-   **Product ID**: `prod_0042`
-   **Event Distribution**: 20% of all events are allocated to this product.
-   **Event Type Weights**:
    -   Views: 95%
    -   Add to Cart: 4%
    -   Purchase: 1%
-   **Purpose**: Simulates a product with high interest but low conversion, which can be used to develop and test alerting mechanisms.

## Utility Commands

### Docker Management

**Stop all services:**
```sh
docker-compose down
```

**Stop all services and remove volumes:**
```sh
docker-compose down -v
```

**View logs for a specific service (e.g., `kafka`):**
```sh
docker-compose logs -f kafka
```

**Restart a specific service:**
```sh
docker-compose restart kafka
```

### Kafka Management

**Delete a topic:**
```sh
docker exec -it kafka kafka-topics --delete --topic ecom_clickstream_raw --bootstrap-server localhost:9092
```

**List all consumer groups:**
```sh
docker exec -it kafka kafka-consumer-groups --list --bootstrap-server localhost:9092
```

**Check topic lag for a consumer group:**
```sh
docker exec -it kafka kafka-consumer-groups --describe --group <group-name> --bootstrap-server localhost:9092
```

## Future Work

### Phase 3: Silver Layer (Planned)
-   Implement a Spark Structured Streaming job.
-   Read data from the Bronze layer Kafka topic.
-   Apply data quality checks, transformations, and event deduplication.
-   Write the processed data to the Silver layer storage.

### Phase 4: Gold Layer (Planned)
-   Aggregate metrics using windowed operations.
-   Calculate key business metrics, such as conversion rates.
-   Develop logic to detect high-interest, low-conversion products.
-   Store results in a format optimized for analytics.

### Phase 5: Orchestration (Planned)
-   Create Airflow DAGs for orchestrating batch processing jobs.
-   Schedule periodic data aggregations.
-   Implement alerting mechanisms for key business events.

## Project Structure

```
Final Project/
├── docker-compose.yml          # Infrastructure configuration
├── ecom_producer.py            # Kafka data producer
├── dags/                       # Directory for Airflow DAGs
├── logs/                       # Directory for Airflow logs
├── plugins/                    # Directory for Airflow plugins
└── README.md                   # Project documentation
```

## Troubleshooting

### Kafka Connection Issues
-   Ensure all Docker services are running by executing `docker-compose ps`.
-   Inspect the Kafka logs for errors: `docker-compose logs -f kafka`.
-   Verify that Zookeeper is healthy by checking its logs: `docker-compose logs -f zookeeper`.

### Python Dependencies
-   If using a virtual environment, ensure it is activated.
-   Confirm that the `kafka-python` library is installed: `pip list | findstr kafka` (Windows) or `pip list | grep kafka` (macOS/Linux).

### Port Conflicts
-   Check for services already using the required ports: `netstat -ano | findstr "9092"`.
-   Stop any conflicting services or modify the port mappings in `docker-compose.yml`.

### Airflow Initialization Failure
-   Confirm that the PostgreSQL container is healthy: `docker-compose logs -f postgres`.
-   Verify that the `airflow-init` container completed its tasks successfully: `docker-compose logs airflow-init`.
-   Attempt to restart the services: `docker-compose restart`.

## Technologies Used

-   **Apache Kafka**: Distributed event streaming platform.
-   **Apache Spark**: Distributed data processing engine.
-   **Apache Airflow**: Workflow orchestration tool.
-   **PostgreSQL**: Relational database for Airflow metadata.
-   **Docker & Docker Compose**: Containerization and service orchestration.
-   **Python**: Language for the data producer and future processing logic.

## Author

-   **Repository**: `realtime-ecommerce-pipeline-kafka-spark-airflow`
-   **Owner**: `KavindaRajapaksha`
-   **Branch**: `development`

---

## Architectural Rationale

### Lambda Architecture Benefits
1.  **Immutability**: The Bronze layer preserves raw, unaltered data, ensuring a permanent source of truth.
2.  **Replayability**: The ability to reprocess historical data from the Bronze layer if business logic or schemas change.
3.  **Scalability**: Kafka's partitioning model enables high-throughput, parallel data processing.
4.  **Fault Tolerance**: The event log provides durability and recovery capabilities.
5.  **Hybrid Processing**: Supports both real-time streaming analytics and batch processing.

### Technology Choices
-   **Kafka**: Selected as the industry standard for high-throughput, scalable event streaming.
-   **Spark**: Chosen for its powerful, unified engine for distributed data processing, including native support for structured streaming.
-   **Airflow**: Employed for its flexibility in orchestrating complex data workflows and its user-friendly interface.
-   **Docker**: Used to ensure consistent, reproducible environments and to simplify deployment.

---

**Last Updated**: November 29, 2025  
**Status**: Phase 2 Complete - Bronze Layer is operational.