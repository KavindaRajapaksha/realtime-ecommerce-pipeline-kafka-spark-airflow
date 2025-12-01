@echo off
REM Script to submit Spark Streaming job - Kappa Architecture (Windows)

echo ======================================================================
echo 🚀 Submitting Spark Streaming Job - Kappa Architecture
echo ======================================================================

docker exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.postgresql:postgresql:42.7.0 ^
  --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints/kappa ^
  --conf spark.sql.shuffle.partitions=3 ^
  --conf spark.streaming.stopGracefullyOnShutdown=true ^
  --driver-memory 1g ^
  --executor-memory 1g ^
  --conf spark.driver.extraJavaOptions="-Divy.home=/tmp/.ivy2" ^
  --conf spark.executor.extraJavaOptions="-Divy.home=/tmp/.ivy2" ^
  /opt/spark/work-dir/spark_streaming_kappa.py


echo.
echo ======================================================================
echo ✅ Kappa Architecture job submitted successfully
echo ======================================================================
echo.
echo 📊 Check alerts: docker exec -it postgres psql -U airflow -d airflow -c "SELECT * FROM flash_sale_alerts ORDER BY alert_timestamp DESC LIMIT 10;"
