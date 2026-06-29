from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from quality_rules import valid_record
from transformations import enrich
from constants import *

# -------------------------------------------------------
# Create Spark Session
# -------------------------------------------------------

builder = (
    SparkSession.builder
    .appName("Manufacturing Silver Layer")
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

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Starting Silver Streaming Pipeline...")
print("=" * 80)

# -------------------------------------------------------
# Read Bronze Delta Stream
# -------------------------------------------------------

bronze_df = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# -------------------------------------------------------
# Split Good / Bad Records
# -------------------------------------------------------

from quality_rules import add_quality_reason

quality_df = add_quality_reason(bronze_df)

good_records = quality_df.filter(valid_record())

bad_records = quality_df.filter(~valid_record())

# -------------------------------------------------------
# Apply Business Transformations
# -------------------------------------------------------

silver_df = enrich(good_records)

# -------------------------------------------------------
# Write Silver Table
# -------------------------------------------------------

silver_query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_SILVER
    )
    .start(SILVER_PATH)
)

# -------------------------------------------------------
# Write Quarantine Table
# -------------------------------------------------------

bad_query = (
    bad_records.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        CHECKPOINT_QUARANTINE
    )
    .start(QUARANTINE_PATH)
)

print("=" * 80)
print("Silver Pipeline Running")
print("Writing clean records to:", SILVER_PATH)
print("Writing bad records to :", QUARANTINE_PATH)
print("=" * 80)

# -------------------------------------------------------
# Wait Forever
# -------------------------------------------------------

spark.streams.awaitAnyTermination()
