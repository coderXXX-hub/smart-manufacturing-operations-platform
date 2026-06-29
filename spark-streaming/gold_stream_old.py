from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

from constants import *

builder = (
    SparkSession.builder
    .appName("Manufacturing Gold")
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

silver = (
    spark.readStream
         .format("delta")
         .load(SILVER_PATH)
)

plant_performance = (

    silver

    .groupBy("plant")

    .agg(

        count("*").alias("production_count"),

        sum("machine_failure").alias("failure_count"),

        round(avg("rpm"),2).alias("avg_rpm"),

        round(avg("torque"),2).alias("avg_torque"),

        round(avg("tool_wear"),2).alias("avg_tool_wear"),

	round(avg("temperature_delta"),2).alias("avg_temperature_delta")

    )

    .withColumn(

        "failure_rate",

        round(

            col("failure_count") /

            col("production_count") * 100,

            2

        )

    )

)

line_performance = (

    silver

    .groupBy(

        "plant",

        "production_line"

    )

    .agg(

        count("*").alias("production_count"),

        sum("machine_failure").alias("failures"),

        round(avg("rpm"),2).alias("avg_rpm"),

        round(avg("torque"),2).alias("avg_torque")

    )

)

machine_health = (

    silver

    .groupBy(

        "machine_id",

        "plant"

    )

    .agg(

        max("event_timestamp").alias("last_seen"),

        max("tool_wear").alias("tool_wear"),

        max("temperature_delta").alias("temperature_delta"),

        last("maintenance_status").alias("maintenance_status"),

        last("machine_status").alias("machine_status")

    )

)

maintenance_dashboard = (

    machine_health
	
    .filter(

    (col("maintenance_status") != "Healthy") |

    (col("machine_status") == "FAILED")

    )

)

shift_performance = (

    silver

    .groupBy("shift")

    .agg(

        count("*").alias("production"),

        sum("machine_failure").alias("failures"),

        round(avg("rpm"),2).alias("avg_rpm"),

        round(avg("tool_wear"),2).alias("avg_tool_wear")

    )

)

product_quality = (

    silver

    .groupBy(

        "product_type"

    )

    .agg(

        count("*").alias("production"),

        sum("machine_failure").alias("failures"),

        round(avg("temperature_delta"),2)

            .alias("avg_temperature_delta")

    )

)

failure_summary = (

    silver

    .filter(

        col("machine_failure")==1

    )

    .groupBy(

        "failure_type"

    )

    .agg(

        count("*").alias("failure_count")

    )

)

ml_feature_store = (

    silver.select(

        "event_timestamp",

        "machine_id",

        "plant",

        "product_type",

        "rpm",

        "torque",

        "tool_wear",

        "temperature_delta",

        "maintenance_status",

        "machine_failure"

    )

)

executive_dashboard = (

    silver

    .agg(

        count("*").alias("total_production"),

        sum("machine_failure").alias("total_failures"),

        round(avg("rpm"),2).alias("avg_rpm"),

        round(avg("tool_wear"),2).alias("avg_tool_wear")

    )

)

pipeline_metrics = (

    silver

    .agg(

        count("*").alias("records_processed"),

        countDistinct("machine_id")

            .alias("unique_machines"),

        countDistinct("plant")

            .alias("plants"),

        countDistinct("production_line")

            .alias("production_lines")

    )

)

maintenance_alerts = (

    silver

    .filter(

        (col("tool_wear") > 170)

        |

        (col("temperature_delta") > 12)

    )

    .select(

        "machine_id",

        "plant",

        "tool_wear",

        "temperature_delta",

        "maintenance_status"

    )

)

oee = (

    silver

    .groupBy("plant")

    .agg(

        count("*").alias("total_events"),

        sum(

            when(

                col("machine_failure")==0,

                1

            ).otherwise(0)

        ).alias("running_events"),

        avg("rpm").alias("performance"),

        avg("tool_wear").alias("quality_proxy")

    )

)

plant_query = (
    plant_performance.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/plant_performance"
    )
    .start("../gold/plant_performance")
)

line_query = (
    line_performance.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/production_line_performance"
    )
    .start("../gold/production_line_performance")
)

machine_query = (
    machine_health.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/machine_health"
    )
    .start("../gold/machine_health")
)

maintenance_query = (
    maintenance_dashboard.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/maintenance_dashboard"
    )
    .start("../gold/maintenance_dashboard")
)

shift_query = (
    shift_performance.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/shift_performance"
    )
    .start("../gold/shift_performance")
)

quality_query = (
    product_quality.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/product_quality"
    )
    .start("../gold/product_quality")
)

failure_query = (
    failure_summary.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/failure_summary"
    )
    .start("../gold/failure_summary")
)

ml_query = (
    ml_feature_store.writeStream
    .outputMode("append")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/ml_feature_store"
    )
    .start("../gold/ml_feature_store")
)

executive_query = (
    executive_dashboard.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/executive_dashboard"
    )
    .start("../gold/executive_dashboard")
)

pipeline_query = (
    pipeline_metrics.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/pipeline_metrics"
    )
    .start("../gold/pipeline_metrics")
)

alerts_query = (
    maintenance_alerts.writeStream
    .outputMode("append")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/maintenance_alerts"
    )
    .start("../gold/maintenance_alerts")
)

oee_query = (
    oee.writeStream
    .outputMode("complete")
    .format("delta")
    .option(
        "checkpointLocation",
        "../checkpoints/gold/oee"
    )
    .start("../gold/oee")
)

spark.streams.awaitAnyTermination()
