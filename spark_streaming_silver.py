"""
Spark Structured Streaming Job - Silver Layer
Reads from Kafka (Bronze), applies transformations, and writes to Silver layer
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, count, sum as _sum,
    current_timestamp, lit, expr, date_format, year, month, dayofmonth, hour
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"  # Internal Docker network
KAFKA_TOPIC = "ecom_clickstream_raw"
CHECKPOINT_LOCATION = "/tmp/spark-checkpoints/silver"
OUTPUT_PATH = "/tmp/silver_layer"

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
    """Create Spark session with Kafka integration"""
    spark = SparkSession.builder \
        .appName("EcomSilverLayer") \
        .master("spark://spark-master:7077") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.shuffle.partitions", "3") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session created successfully")
    return spark

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

def transform_bronze_to_silver(df):
    """Apply Silver layer transformations"""
    print("🔄 Applying Silver layer transformations...")
    
    # Parse JSON from Kafka value
    parsed_df = df.select(
        col("key").cast("string").alias("kafka_key"),
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    
    # Flatten the structure
    flattened_df = parsed_df.select(
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
        col("kafka_key"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        current_timestamp().alias("processing_timestamp")
    )
    
    # Data Quality Checks and Transformations
    cleaned_df = flattened_df \
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
    
    print("✅ Transformations applied successfully")
    return cleaned_df

def write_to_silver(df):
    """Write to Silver layer with partitioning"""
    print(f"💾 Writing to Silver layer: {OUTPUT_PATH}")
    
    query = df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", OUTPUT_PATH) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .partitionBy("event_date", "event_type") \
        .trigger(processingTime="20 seconds") \
        .start()
    
    print("✅ Silver layer streaming query started")
    print(f"📊 Query ID: {query.id}")
    print(f"📍 Checkpoint: {CHECKPOINT_LOCATION}")
    print(f"📂 Output: {OUTPUT_PATH}")
    
    return query

def write_to_console(df):
    """Write streaming data to console for debugging"""
    query = df \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return query

def main():
    """Main execution function"""
    print("=" * 80)
    print("🚀 Starting Spark Structured Streaming - Silver Layer Processing")
    print("=" * 80)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Read from Kafka (Bronze layer)
        bronze_df = read_from_kafka(spark)
        
        # Transform to Silver
        silver_df = transform_bronze_to_silver(bronze_df)
        
        # Write to Silver layer (Parquet files)
        silver_query = write_to_silver(silver_df)
        
        # Also write to console for monitoring (optional)
        console_query = write_to_console(
            silver_df.select(
                "event_id", "user_id", "product_id", "event_type", 
                "event_timestamp", "price", "is_high_value"
            )
        )
        
        print("\n" + "=" * 80)
        print("✅ All streaming queries started successfully!")
        print("=" * 80)
        print("\n📊 Monitoring Instructions:")
        print("   - View Spark UI: http://localhost:8080")
        print("   - Check application details in Spark Master UI")
        print("   - Monitor console output for real-time events")
        print("   - Press Ctrl+C to stop gracefully")
        print("=" * 80 + "\n")
        
        # Wait for termination
        silver_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("\n⚠️  Received interrupt signal. Stopping gracefully...")
        spark.streams.active[0].stop()
        print("✅ Streaming job stopped successfully")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
        raise
    finally:
        spark.stop()
        print("✅ Spark session closed")

if __name__ == "__main__":
    main()
