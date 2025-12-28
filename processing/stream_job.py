import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, expr, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SPARK_VERSION = "3.5.1"
SCALA_BINARY = "2.12"

os.environ['PYSPARK_SUBMIT_ARGS'] = (
    f'--packages org.apache.spark:spark-sql-kafka-0-10_{SCALA_BINARY}:{SPARK_VERSION},'
    f'com.datastax.spark:spark-cassandra-connector_{SCALA_BINARY}:{SPARK_VERSION} '
    '--conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" '
    'pyspark-shell'
)

# Create Spark Session
spark = SparkSession.builder \
    .appName("HealthInsightProcessor") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema Definition
schema = StructType([
    StructField("patient_id", StringType(), True),
    StructField("metric_name", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "patient-vitals") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Process JSON
vitals_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast("timestamp"))

# Alert Logic
alerts_df = vitals_df.withColumn("alert_type", 
    when((col("metric_name") == "heart_rate") & (col("value") > 120), "TACHYCARDIA")
    .when((col("metric_name") == "spo2") & (col("value") < 92), "HYPOXIA")
    .when((col("metric_name") == "temperature") & (col("value") > 39), "FEVER")
    .otherwise(None)
).filter(col("alert_type").isNotNull())

alerts_df = alerts_df.withColumn("day", to_date(col("timestamp")))

# Write to Cassandra (Vitals)
vitals_query = vitals_df.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "health_insight") \
    .option("table", "patient_vitals") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_vitals") \
    .start()

# Write to Cassandra (Alerts)
alerts_query = alerts_df.select(
    expr("uuid()").alias("alert_id"),
    "patient_id",
    "metric_name",
    col("value").alias("observed_value"),
    "alert_type",
    "timestamp",
    expr("'ACTIVE'").alias("status")
).writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "health_insight") \
    .option("table", "health_alerts") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_alerts") \
    .start()

# Write to Cassandra (Alerts by Day)
alerts_by_day_query = alerts_df.select(
    "day",
    "timestamp",
    expr("uuid()").alias("alert_id"),
    "patient_id",
    "metric_name",
    col("value").alias("observed_value"),
    "alert_type",
    expr("'ACTIVE'").alias("status")
).writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "health_insight") \
    .option("table", "health_alerts_by_day") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_alerts_by_day") \
    .start()

# Write to Cassandra (Alerts by Patient)
alerts_by_patient_query = alerts_df.select(
    "patient_id",
    "timestamp",
    expr("uuid()").alias("alert_id"),
    "metric_name",
    col("value").alias("observed_value"),
    "alert_type",
    expr("'ACTIVE'").alias("status"),
    "day",
).writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "health_insight") \
    .option("table", "health_alerts_by_patient") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_alerts_by_patient") \
    .start()

print("🚀 Streaming Engine Started. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()