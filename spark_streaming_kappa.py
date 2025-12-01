"""
Spark Structured Streaming - Kappa Architecture
Unified stream processing: Kafka → Transformations → PostgreSQL Alerts
Single pipeline for all transformations and analytics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, count, sum as _sum,
    current_timestamp, lit, expr, date_format, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "ecom_clickstream_data"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/kappa"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"
ALERT_TABLE = "flash_sale_alerts"

# Alert Thresholds
VIEW_THRESHOLD = 100
PURCHASE_THRESHOLD = 5
WINDOW_DURATION = "10 minutes"
SLIDE_DURATION = "1 minute"
WATERMARK_DELAY = "5 minutes"

# Define schema for incoming JSON data
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("price", DoubleType(), True)
])

def create_spark_session():
    """Create Spark session with Kafka and PostgreSQL integration"""
    spark = SparkSession.builder \
        .appName("EcomKappaArchitecture") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.postgresql:postgresql:42.7.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session created successfully")
    return spark

def initialize_postgres_table():
    """Create the alerts table in PostgreSQL if it doesn't exist"""
    print("📊 Initializing PostgreSQL alerts table...")
    
    try:
        import subprocess
        
        create_table_sql = f"""CREATE TABLE IF NOT EXISTS {ALERT_TABLE} (
            alert_id SERIAL PRIMARY KEY,
            product_id VARCHAR(50) NOT NULL,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            total_views INTEGER NOT NULL,
            total_purchases INTEGER NOT NULL,
            conversion_rate DOUBLE PRECISION,
            alert_timestamp TIMESTAMP NOT NULL,
            alert_triggered BOOLEAN DEFAULT TRUE
        );"""
        
        cmd = [
            "docker", "exec", "-i", "postgres",
            "psql", "-U", POSTGRES_USER, "-d", "airflow",
            "-c", create_table_sql
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0 or "already exists" in result.stderr:
            print(f"✅ Table '{ALERT_TABLE}' initialized successfully")
        else:
            print(f"⚠️  Table creation result: {result.stderr}")
            
    except Exception as e:
        print(f"⚠️  Table might already exist or error occurred: {e}")

def read_from_kafka(spark):
    """Read streaming data from Kafka topic"""
    print(f"📖 Reading from Kafka topic: {KAFKA_TOPIC}")
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    print("✅ Kafka stream connected")
    return df

def transform_stream(df):
    """Apply all transformations in single pass (Kappa Architecture)"""
    print("🔄 Applying transformations...")
    
    # Parse JSON from Kafka
    parsed_df = df.select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    
    # Flatten and cleanse
    cleaned_df = parsed_df.select(
        col("data.event_id"),
        col("data.user_id"),
        col("data.product_id"),
        col("data.event_type"),
        to_timestamp(col("data.timestamp")).alias("event_timestamp"),
        col("data.session_id"),
        col("data.product_category"),
        col("data.device_type"),
        col("data.page_url"),
        col("data.price"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        current_timestamp().alias("processing_timestamp")
    ) \
    .filter(col("event_id").isNotNull()) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("product_id").isNotNull()) \
    .filter(col("event_type").isNotNull()) \
    .filter(col("event_timestamp").isNotNull()) \
    .filter(col("price") > 0) \
    .withColumn("is_high_value", expr("CASE WHEN price > 500 THEN true ELSE false END")) \
    .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
    .withColumn("event_hour", hour(col("event_timestamp"))) \
    .dropDuplicates(["event_id"])
    
    print("✅ Data cleaning completed")
    return cleaned_df

def apply_windowing_and_alerts(df):
    """Apply windowing and generate flash sale alerts"""
    print(f"🔄 Applying windowing logic: {WINDOW_DURATION} window, {SLIDE_DURATION} slide")
    
    # Apply watermark and windowing
    windowed_df = df \
        .withWatermark("event_timestamp", WATERMARK_DELAY) \
        .groupBy(
            window(col("event_timestamp"), WINDOW_DURATION, SLIDE_DURATION),
            col("product_id")
        ) \
        .agg(
            _sum(expr("CASE WHEN event_type = 'view' THEN 1 ELSE 0 END")).alias("total_views"),
            _sum(expr("CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END")).alias("total_purchases")
        )
    
    # Generate alerts
    alert_df = windowed_df \
        .select(
            col("product_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_views"),
            col("total_purchases"),
            expr("CASE WHEN total_views > 0 THEN (total_purchases * 100.0 / total_views) ELSE 0 END").alias("conversion_rate")
        ) \
        .filter(col("total_views") > VIEW_THRESHOLD) \
        .filter(col("total_purchases") < PURCHASE_THRESHOLD) \
        .withColumn("alert_timestamp", current_timestamp()) \
        .withColumn("alert_triggered", lit(True))
    
    print("✅ Windowing and alert logic applied")
    return alert_df

def write_to_postgres(df, epoch_id):
    """Write alerts to PostgreSQL table"""
    try:
        df.write \
            .format("jdbc") \
            .option("url", POSTGRES_URL) \
            .option("dbtable", ALERT_TABLE) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        count = df.count()
        if count > 0:
            print(f"🚨 ALERT! {count} flash sale opportunity detected!")
            df.select("product_id", "total_views", "total_purchases", "conversion_rate").show(truncate=False)
    except Exception as e:
        print(f"❌ Error writing to PostgreSQL: {e}")

def write_alerts_stream(df):
    """Write alerts to PostgreSQL using foreachBatch"""
    print(f"💾 Writing alerts to PostgreSQL: {ALERT_TABLE}")
    
    query = df \
        .writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("update") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="1 minute") \
        .start()
    
    print("✅ Alert streaming query started")
    print(f"📊 Query ID: {query.id}")
    print(f"📍 Checkpoint: {CHECKPOINT_LOCATION}")
    print(f"🗄️  Database: {POSTGRES_URL}")
    print(f"📋 Table: {ALERT_TABLE}")
    
    return query

def write_to_console(df):
    """Write alerts to console for monitoring"""
    query = df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()
    
    return query

def main():
    """Main execution function - Kappa Architecture"""
    print("=" * 80)
    print("🚀 Starting Spark Structured Streaming - Kappa Architecture")
    print("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Initialize PostgreSQL table
        initialize_postgres_table()
        
        # Read from Kafka
        raw_stream = read_from_kafka(spark)
        
        # Transform stream (cleansing, enrichment)
        cleaned_stream = transform_stream(raw_stream)
        
        # Apply windowing and generate alerts
        alert_stream = apply_windowing_and_alerts(cleaned_stream)
        
        # Write to PostgreSQL
        postgres_query = write_alerts_stream(alert_stream)
        
        # Also write to console for monitoring
        console_query = write_to_console(alert_stream)
        
        print("\n" + "=" * 80)
        print("✅ Kappa Architecture streaming pipeline started successfully!")
        print("=" * 80)
        print("\n📊 Monitoring Instructions:")
        print("   - View Spark UI: http://localhost:8080")
        print("   - Check Spark Application UI: http://localhost:4040")
        print("   - Monitor console for flash sale alerts")
        print("   - Query PostgreSQL for alert history")
        print("\n🔍 Check Alerts in PostgreSQL:")
        print(f"   docker exec -it postgres psql -U airflow -d airflow -c 'SELECT * FROM {ALERT_TABLE} ORDER BY alert_timestamp DESC LIMIT 10;'")
        print("\n⚠️  Press Ctrl+C to stop gracefully")
        print("=" * 80 + "\n")
        
        # Wait for termination
        postgres_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⚠️  Received interrupt signal. Stopping gracefully...")
        for query in spark.streams.active:
            query.stop()
        print("✅ Streaming job stopped successfully")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("✅ Spark session closed")

if __name__ == "__main__":
    main()
