from pyspark.sql.types import *

manufacturing_schema = StructType([

    StructField("event_time", StringType()),
    StructField("plant", StringType()),
    StructField("production_line", StringType()),
    StructField("work_center", StringType()),
    StructField("shift", StringType()),
    StructField("operator_id", StringType()),
    StructField("production_order", StringType()),
    StructField("machine_id", StringType()),
    StructField("product_type", StringType()),
    StructField("air_temperature", DoubleType()),
    StructField("process_temperature", DoubleType()),
    StructField("rpm", IntegerType()),
    StructField("torque", DoubleType()),
    StructField("tool_wear", IntegerType()),
    StructField("machine_failure", IntegerType()),
    StructField("failure_type", StringType())

])
