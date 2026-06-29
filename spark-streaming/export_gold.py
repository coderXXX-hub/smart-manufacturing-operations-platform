from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("Export Gold")
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

tables = [
    "executive_dashboard",
    "plant_performance",
    "machine_health",
    "failure_summary",
    "oee"
]

for table in tables:

    print(f"Exporting {table}...")

    df = spark.read.format("delta").load(f"../gold/{table}")

    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("header", True)
          .csv(f"../dashboard/data/{table}")
    )

print("Export Complete")

