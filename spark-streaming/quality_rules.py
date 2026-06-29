from pyspark.sql.functions import *

def add_quality_reason(df):

    return (

        df

        .withColumn(

            "quality_reason",

            when(col("machine_id").isNull(),"Missing Machine ID")

            .when(col("event_time").isNull(),"Missing Event Time")

            .when(col("rpm") <= 0,"Invalid RPM")

            .when(col("torque") <= 0,"Invalid Torque")

            .when(col("tool_wear") < 0,"Invalid Tool Wear")

            .when(~col("air_temperature").between(250,350),
                  "Invalid Air Temperature")

            .when(~col("process_temperature").between(250,350),
                  "Invalid Process Temperature")

            .otherwise(None)

        )

    )

def valid_record():

    return col("quality_reason").isNull()
