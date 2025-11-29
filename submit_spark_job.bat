@echo off
REM Script to submit Spark Streaming job to Spark cluster (Windows)

echo ======================================================================
echo 🚀 Submitting Spark Streaming Job - Silver Layer
echo ======================================================================

docker exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0 ^
  --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints/silver ^
  --conf spark.sql.shuffle.partitions=3 ^
  --conf spark.streaming.stopGracefullyOnShutdown=true ^
  --driver-memory 1g ^
  --executor-memory 1g ^
  --conf spark.driver.extraJavaOptions="-Divy.home=/tmp/.ivy2" ^
  --conf spark.executor.extraJavaOptions="-Divy.home=/tmp/.ivy2" ^
  /opt/spark/work-dir/spark_streaming_silver.py


echo.
echo ======================================================================
echo ✅ Job submitted successfully
echo ======================================================================
