from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os
from pyspark.sql.functions import from_json
from schema import manufacturing_schema

# -----------------------------
# Load Configuration
# -----------------------------
load_dotenv("../producer/.env")

BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
TOPIC = os.getenv("TOPIC")

print("=" * 60)
print("Connecting to Kafka...")
print("Bootstrap:", BOOTSTRAP_SERVER)
print("Topic:", TOPIC)
print("=" * 60)

# -----------------------------
# Create Spark Session
# -----------------------------
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("Manufacturing Bronze Streaming")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6"
    ]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Spark Started Successfully!")

# -----------------------------
# Read Kafka Stream
# -----------------------------
kafka_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";'
        )
        .load()
)

from pyspark.sql.functions import current_timestamp, lit

events = kafka_df.select(
    col("timestamp").alias("kafka_timestamp"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    current_timestamp().alias("ingestion_timestamp"),
    lit("MES").alias("source_system"),
    col("value").cast("string").alias("json")
)

parsed = events.select(
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "ingestion_timestamp",
    "source_system",
    from_json(col("json"), manufacturing_schema).alias("data")
)

bronze_df = parsed.select(
    "kafka_timestamp",
    "kafka_partition",
    "kafka_offset",
    "ingestion_timestamp",
    "source_system",
    "data.*"
)


query = (
    bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "../checkpoints/bronze")
        .start("../bronze")
)

query.awaitTermination()


