from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Manufacturing Analytics")
    .master("local[*]")
    .getOrCreate()
)

print("=" * 60)
print("Spark Version:", spark.version)
print("=" * 60)

spark.stop()
