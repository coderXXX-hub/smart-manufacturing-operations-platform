from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("Dashboard")
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


def load(path):
    return spark.read.format("delta").load(path).toPandas()


def load_executive():
    return load("../gold/executive_dashboard")


def load_plant_performance():
    return load("../gold/plant_performance")


def load_machine_health():
    return load("../gold/machine_health")


def load_failure_summary():
    return load("../gold/failure_summary")


def load_oee():
    return load("../gold/oee")
