"""
gold_metrics.py

Reusable KPI functions for the Gold Layer of the
Smart Manufacturing Lakehouse.

Author: Soumya Vaka
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    approx_count_distinct,
    last,
    lit,
    round,
    sum,
    when,
    current_timestamp
)


# ==========================================================
# Plant Performance
# ==========================================================

def plant_performance(df: DataFrame) -> DataFrame:

    return (

        df

        .groupBy("plant")

        .agg(

            count("*").alias("production_count"),

            sum("machine_failure").alias("failure_count"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("torque"), 2).alias("avg_torque"),

            round(avg("tool_wear"), 2).alias("avg_tool_wear"),

            round(avg("temperature_delta"), 2)
                .alias("avg_temperature_delta")

        )

        .withColumn(

            "failure_rate",

            round(

                col("failure_count")
                / col("production_count") * 100,

                2

            )

        )

    )


# ==========================================================
# Production Line Performance
# ==========================================================

def production_line_performance(df: DataFrame) -> DataFrame:

    return (

        df

        .groupBy(

            "plant",
            "production_line"

        )

        .agg(

            count("*").alias("production_count"),

            sum("machine_failure").alias("failure_count"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("torque"), 2).alias("avg_torque"),

            round(avg("tool_wear"), 2).alias("avg_tool_wear")

        )

        .withColumn(

            "failure_rate",

            round(

                col("failure_count")
                / col("production_count") * 100,

                2

            )

        )

    )


# ==========================================================
# Machine Health
# ==========================================================

def machine_health(df: DataFrame) -> DataFrame:

    return (

        df

        .groupBy(

            "machine_id",
            "plant"

        )

        .agg(

            last(
                "event_timestamp",
                ignorenulls=True
            ).alias("last_seen"),

            round(
                avg("tool_wear"),
                2
            ).alias("avg_tool_wear"),

            round(
                avg("temperature_delta"),
                2
            ).alias("avg_temperature_delta"),

            last(
                "maintenance_status",
                ignorenulls=True
            ).alias("maintenance_status"),

            last(
                "machine_status",
                ignorenulls=True
            ).alias("machine_status")

        )

    )


# ==========================================================
# Maintenance Dashboard
# ==========================================================

def maintenance_dashboard(df: DataFrame) -> DataFrame:

    machines = machine_health(df)

    return (

        machines

        .filter(

            (col("maintenance_status") != "Healthy")

            |

            (col("machine_status") == "FAILED")

        )

    )


# ==========================================================
# Shift Performance
# ==========================================================

def shift_performance(df: DataFrame) -> DataFrame:

    return (

        df

        .groupBy("shift")

        .agg(

            count("*").alias("production_count"),

            sum("machine_failure").alias("failure_count"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("torque"), 2).alias("avg_torque"),

            round(avg("tool_wear"), 2).alias("avg_tool_wear")

        )

        .withColumn(

            "failure_rate",

            round(

                col("failure_count")

                / col("production_count")

                * 100,

                2

            )

        )

    )


# ==========================================================
# Product Quality
# ==========================================================

def product_quality(df: DataFrame) -> DataFrame:

    return (

        df

        .groupBy(

            "product_type"

        )

        .agg(

            count("*").alias("production_count"),

            sum("machine_failure").alias("failure_count"),

            round(

                avg("temperature_delta"),

                2

            ).alias("avg_temperature_delta"),

            round(

                avg("tool_wear"),

                2

            ).alias("avg_tool_wear")

        )

        .withColumn(

            "failure_rate",

            round(

                col("failure_count")

                /

                col("production_count")

                * 100,

                2

            )

        )

    )

# ==========================================================
# Failure Summary
# ==========================================================

def failure_summary(df: DataFrame) -> DataFrame:

    return (

        df

        .filter(col("machine_failure") == 1)

        .groupBy(

            "failure_type"

        )

        .agg(

            count("*").alias("failure_count"),

            approx_count_distinct("machine_id").alias("machines_affected"),           

            round(avg("tool_wear"), 2).alias("avg_tool_wear"),

            round(avg("temperature_delta"), 2)
                .alias("avg_temperature_delta")

        )

    )


# ==========================================================
# ML Feature Store
# ==========================================================

def ml_feature_store(df: DataFrame) -> DataFrame:

    return (

        df.select(

            "event_timestamp",

            "machine_id",

            "plant",

            "production_line",

            "shift",

            "product_type",

            "rpm",

            "torque",

            "tool_wear",

            "air_temperature",

            "process_temperature",

            "temperature_delta",

            "maintenance_status",

            "machine_status",

            "machine_failure",

            "failure_type"

        )

    )


# ==========================================================
# Executive Dashboard
# ==========================================================

def executive_dashboard(df: DataFrame) -> DataFrame:

    return (

        df

        .agg(

            count("*").alias("total_production"),

            approx_count_distinct("machine_id").alias("total_machines"),

            approx_count_distinct("plant").alias("plants"),

            approx_count_distinct("production_line")
                .alias("production_lines"),

            sum("machine_failure").alias("total_failures"),

            round(avg("rpm"), 2).alias("avg_rpm"),

            round(avg("torque"), 2).alias("avg_torque"),

            round(avg("tool_wear"), 2)
                .alias("avg_tool_wear"),

            round(avg("temperature_delta"), 2)
                .alias("avg_temperature_delta")

        )

        .withColumn(

            "failure_rate",

            round(

                col("total_failures")

                /

                col("total_production")

                * 100,

                2

            )

        )

        .withColumn(

            "report_generated",

            current_timestamp()

        )

    )


# ==========================================================
# Pipeline Metrics
# ==========================================================

def pipeline_metrics(df: DataFrame) -> DataFrame:

    return (

        df

        .agg(

            count("*").alias("records_processed"),

            approx_count_distinct("machine_id")                
                .alias("unique_machines"),

            approx_count_distinct("plant")
                .alias("plants"),

            approx_count_distinct("production_line")
                .alias("production_lines"),

            sum("machine_failure")
                .alias("failed_records"),

            round(avg("temperature_delta"), 2)
                .alias("avg_temperature_delta")

        )

        .withColumn(

            "pipeline_name",

            lit("Manufacturing Gold")

        )

        .withColumn(

            "processed_time",

            current_timestamp()

        )

    )


# ==========================================================
# Maintenance Alerts
# ==========================================================

def maintenance_alerts(df: DataFrame) -> DataFrame:

    return (

        df

        .filter(

            (col("tool_wear") >= 170)

            |

            (col("temperature_delta") >= 12)

            |

            (col("machine_failure") == 1)

        )

        .select(

            "event_timestamp",

            "plant",

            "production_line",

            "machine_id",

            "tool_wear",

            "temperature_delta",

            "maintenance_status",

            "machine_status",

            "failure_type"

        )

    )


# ==========================================================
# Overall Equipment Effectiveness (OEE)
# ==========================================================

def oee(df: DataFrame) -> DataFrame:

    return (

        df

        .groupBy("plant")

        .agg(

            count("*").alias("total_events"),

            sum(

                when(

                    col("machine_failure") == 0,

                    1

                ).otherwise(0)

            ).alias("running_events"),

            round(avg("rpm"), 2)
                .alias("performance"),

            round(avg("tool_wear"), 2)
                .alias("quality_proxy")

        )

        .withColumn(

            "availability",

            round(

                col("running_events")

                /

                col("total_events")

                * 100,

                2

            )

        )

    )
