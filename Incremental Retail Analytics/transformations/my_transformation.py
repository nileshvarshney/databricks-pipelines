import dlt
from pyspark.sql.functions import col, to_date, expr
from pyspark.sql.types import *

#SRC_TAXI = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/"
SRC_TAXI = "/databricks-datasets/nyctaxi/tripdata/yellow/"

# infer schema at runtime is ok, but we’ll define a minimal subset to be explicit
schema = StructType([
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

@dlt.table(comment="Bronze taxi trips (streaming).")
def taxi_raw():
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .schema(schema)
            .load(SRC_TAXI))

@dlt.table(comment="Silver taxi trips with filters & derived date.")
@dlt.expect("valid_amount", "total_amount IS NOT NULL AND total_amount >= 0")
@dlt.expect_or_drop("distance_ok", "trip_distance >= 0")
def taxi_clean():
    return (spark.read.table("taxi_raw")
            .withColumn("trip_date", to_date("tpep_pickup_datetime"))
            .filter(col("trip_date").isNotNull()))

@dlt.table(comment="Gold: daily total revenue.")
def taxi_daily_revenue():
    return (spark.read.table("taxi_clean")
            .groupBy("trip_date")
            .agg(expr("sum(total_amount) as total_revenue"))
            .orderBy("trip_date"))
