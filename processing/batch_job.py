
import argparse
import os
from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, countDistinct, max as spark_max, min as spark_min, to_date


SPARK_VERSION = "3.5.1"
SCALA_BINARY = "2.12"


def _parse_args() -> argparse.Namespace:
	p = argparse.ArgumentParser(description="HealthInsight daily batch analytics")
	p.add_argument(
		"--stat-date",
		default=None,
		help="Target date (UTC) in YYYY-MM-DD. Default: today (UTC).",
	)
	p.add_argument(
		"--cassandra-host",
		default="localhost",
		help="Cassandra host (default: localhost)",
	)
	p.add_argument(
		"--keyspace",
		default="health_insight",
		help="Cassandra keyspace (default: health_insight)",
	)
	p.add_argument(
		"--source-table",
		default="patient_vitals",
		help="Source vitals table (default: patient_vitals)",
	)
	p.add_argument(
		"--target-table",
		default="daily_health_summary",
		help="Target summary table (default: daily_health_summary)",
	)
	return p.parse_args()


def _get_target_date(stat_date: str | None) -> date:
	if not stat_date:
		return datetime.now().date()
	return datetime.strptime(stat_date, "%Y-%m-%d").date()


def main() -> None:
	args = _parse_args()
	target = _get_target_date(args.stat_date)

	# Spark Cassandra connector
	os.environ["PYSPARK_SUBMIT_ARGS"] = (
		f"--packages com.datastax.spark:spark-cassandra-connector_{SCALA_BINARY}:{SPARK_VERSION} "
		'--conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED" '
		"pyspark-shell"
	)

	spark = (
		SparkSession.builder.appName("HealthInsightBatchAnalytics")
		.master("local[*]")
		.config("spark.driver.host", "127.0.0.1")
		.config("spark.driver.bindAddress", "127.0.0.1")
		.config("spark.cassandra.connection.host", args.cassandra_host)
		.config("spark.sql.shuffle.partitions", "2")
		.getOrCreate()
	)
	spark.sparkContext.setLogLevel("ERROR")

	try:
		vitals = (
			spark.read.format("org.apache.spark.sql.cassandra")
			.options(keyspace=args.keyspace, table=args.source_table)
			.load()
		)

		# Derive stat_date from timestamp and filter.
		vitals = vitals.withColumn("stat_date", to_date(col("timestamp")))
		filtered = vitals.filter(col("stat_date") == target.isoformat())

		if filtered.rdd.isEmpty():
			print(f"No vitals found for stat_date={target}. Nothing to write.")
			return

		summary = (
			filtered.groupBy("stat_date", "metric_name")
			.agg(
				avg(col("value")).alias("avg_value"),
				spark_min(col("value")).alias("min_value"),
				spark_max(col("value")).alias("max_value"),
				countDistinct(col("patient_id")).cast("int").alias("patient_count"),
			)
			.select(
				col("stat_date").cast("date").alias("stat_date"),
				col("metric_name"),
				col("avg_value").cast("double"),
				col("min_value").cast("double"),
				col("max_value").cast("double"),
				col("patient_count"),
			)
		)

		# Write to Cassandra (batch)
		(
			summary.write.format("org.apache.spark.sql.cassandra")
			.options(keyspace=args.keyspace, table=args.target_table)
			.mode("append")
			.save()
		)

		print(f"Wrote daily summary for {target} into {args.keyspace}.{args.target_table}")
		summary.orderBy("metric_name").show(50, truncate=False)
	finally:
		spark.stop()


if __name__ == "__main__":
	main()
