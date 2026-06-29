from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging

from constants import *

from gold_metrics import *

# ----------------------------------------------------------
# Logging
# ----------------------------------------------------------

logging.basicConfig(

    level=logging.INFO,

    format="%(asctime)s %(levelname)s %(message)s"

)

logger = logging.getLogger(__name__)

# ----------------------------------------------------------
# Spark Session
# ----------------------------------------------------------

builder = (

    SparkSession.builder

    .appName("Manufacturing Gold Layer")

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

logger.info("=" * 80)
logger.info("Starting Gold Streaming Pipeline")
logger.info("=" * 80)

# ----------------------------------------------------------
# Read Silver Stream
# ----------------------------------------------------------

silver = (

    spark.readStream

    .format("delta")

    .load(SILVER_PATH)

)

# ----------------------------------------------------------
# KPI DataFrames
# ----------------------------------------------------------

plant_df = plant_performance(silver)

line_df = production_line_performance(silver)

machine_df = machine_health(silver)

maintenance_df = maintenance_dashboard(silver)

shift_df = shift_performance(silver)

product_df = product_quality(silver)

failure_df = failure_summary(silver)

ml_df = ml_feature_store(silver)

executive_df = executive_dashboard(silver)

pipeline_df = pipeline_metrics(silver)

alerts_df = maintenance_alerts(silver)

oee_df = oee(silver)

logger.info("Created all Gold KPI DataFrames")

# ----------------------------------------------------------
# Start Streams
# ----------------------------------------------------------

queries = []

queries.append(

    plant_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_PLANT

    )

    .start(GOLD_PLANT)

)

queries.append(

    line_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_LINE

    )

    .start(GOLD_LINE)

)

queries.append(

    machine_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_MACHINE

    )

    .start(GOLD_MACHINE)

)

queries.append(

    maintenance_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_MAINTENANCE

    )

    .start(GOLD_MAINTENANCE)

)

queries.append(

    shift_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_SHIFT

    )

    .start(GOLD_SHIFT)

)

queries.append(

    product_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_PRODUCT

    )

    .start(GOLD_PRODUCT)

)

queries.append(

    failure_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_FAILURE

    )

    .start(GOLD_FAILURE)

)

queries.append(

    ml_df.writeStream

    .format("delta")

    .outputMode("append")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_ML

    )

    .start(GOLD_ML)

)

queries.append(

    executive_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_EXECUTIVE

    )

    .start(GOLD_EXECUTIVE)

)

queries.append(

    pipeline_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_PIPELINE

    )

    .start(GOLD_PIPELINE)

)

queries.append(

    alerts_df.writeStream

    .format("delta")

    .outputMode("append")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_ALERTS

    )

    .start(GOLD_ALERTS)

)

queries.append(

    oee_df.writeStream

    .format("delta")

    .outputMode("complete")

    .option(

        "checkpointLocation",

        CHECKPOINT_GOLD_OEE

    )

    .start(GOLD_OEE)

)

logger.info("=" * 80)
logger.info("Gold Layer Started Successfully")
logger.info(f"Running {len(queries)} streaming queries")
logger.info("=" * 80)

try:

    spark.streams.awaitAnyTermination()

except KeyboardInterrupt:

    logger.info("Stopping Gold Pipeline...")

finally:

    for q in queries:

        q.stop()

    spark.stop()

    logger.info("Gold Pipeline Stopped")
