from pyspark.sql.functions import *

def enrich(df):

    return (

        df

        .withColumn(
            "event_timestamp",
            to_timestamp("event_time")
        )

        .withColumn(
            "temperature_delta",
            col("process_temperature") -
            col("air_temperature")
        )

        .withColumn(
            "maintenance_status",

            when(col("tool_wear") < 50, "Healthy")

            .when(col("tool_wear") < 150, "Monitor")

            .otherwise("Replace Soon")
        )

        .withColumn(
            "machine_status",

            when(col("machine_failure")==1, "FAILED")

            .otherwise("RUNNING")
        )

    )
