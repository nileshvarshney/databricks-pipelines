import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

BASE_PATH = "/databricks-datasets/retail-org"

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_price", StringType(), True),  
    StructField("EAN13", StringType(), True),
    StructField("EAN5", StringType(), True),
    StructField("product_unit", StringType(), True)
]) 

item_schema = ArrayType(StructType([
    StructField("curr",  StringType()),
    StructField("id",    StringType()),
    StructField("name",  StringType()),
    StructField("price", DoubleType()),
    StructField("qty",   IntegerType()),
    StructField("unit",  StringType()),
]))

@dlt.table(
    comment="Bronze customers from retail-org."
)
def customer_bronze_retail_org():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load(f"{BASE_PATH}/customers/")    
    )

@dlt.table(
    comment="Bronze products from retail-org."
)
def products_bronze_retail_org():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep",";")
        .option("header", "true")
        .schema(products_schema)
        .load(f"{BASE_PATH}/products/")    
    )

@dlt.table(
    comment="Bronze sales from retail-org."
)
def sales_orders_bronze_retail_org():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{BASE_PATH}/sales_orders/")    
    )

@dlt.table(
    comment="Silver:  sales_orders_prepared."
)
def sales_orders_prepared():
    df =  spark.readStream.table("sales_orders_bronze_retail_org")
    parsed = (df
        .withColumn("ordered_products_array", from_json(col("ordered_products"), item_schema))
        .select(
                col("order_number"),
                col("customer_id"),
                col("customer_name"),
                col("number_of_line_items"),
                (when(col("order_datetime") != "", to_timestamp(from_unixtime(col("order_datetime").cast("BIGINT") / 1000)))
                .otherwise(lit(None))
                .alias("order_datetime")),
                col("ordered_products_array.curr").alias("curr"),
                col("ordered_products_array.id").alias("id"),
                col("ordered_products_array.name").alias("name"),
                col("ordered_products_array.price").alias("price"),
                col("ordered_products_array.qty").alias("qty"),
                col("ordered_products_array.unit").alias("unit"),
                ).withColumn("items",
                    explode(arrays_zip(
                        col("curr"),
                        col("id"),
                        col("name"),
                        col("price"),
                        col("qty"),
                        col("unit")))
                ).select("order_number", "order_datetime", "customer_id", "customer_name","items.id", "items.name", "items.price", "items.qty",  "items.curr", "items.unit")
    )
    return parsed

@dlt.table(
    comment="Silver:  products_prepared."
)
def products_prepared():
    return (
        spark.readStream.table("products_bronze_retail_org")
        .select(
            "product_id", 
            "product_category", 
            "product_name", 
            round(col("sales_price").cast("double"), 2).alias("sales_price"),
            "EAN13", "EAN5", "product_unit")
    )

@dlt.table(
    comment="Silver:  customers_prepared."
)
def customers_prepared():
    return (
        spark.readStream.table("customer_bronze_retail_org")
        .select(
            col("customer_id").alias("customer_id"), 
            col("customer_name").alias("customer_name"),
            col("tax_code").alias("tax_code"),
            regexp_replace(col("tax_id"),r"\.0$","").alias("tax_id"),
            col("state").alias("state"),
            col("city").alias("city"),
            regexp_replace(col("postcode"),r"\.0$","").alias("postcode"),
            col("street").alias("street"),
            regexp_replace(col("number"),r"\.0$","").alias("number"),
            col("unit").alias("unit"),
            col("region").alias("region"),
            when(
                col("valid_from") != "", 
                to_timestamp(from_unixtime(col("valid_from").cast("BIGINT") / 1000))
                ).otherwise(lit(None)).alias("valid_from"),
            
            when(
                col("valid_to") != "", 
                to_timestamp(from_unixtime(regexp_replace(col("valid_to"),r"\.0$","").cast("BIGINT").cast("BIGINT") / 1000))
                ).otherwise(lit(None)).alias("valid_to"),
            col("loyalty_segment").alias("loyalty_segment")
        )
    )

@dlt.table(
    comment="Gold:  sales_orders."
)
def sales_orders():
    c = spark.readStream.table("customers_prepared").alias("c")
    p = spark.readStream.table("products_prepared").alias("p")
    s = spark.readStream.table("sales_orders_prepared").alias("s")
    df = (
        c.join(s , c.customer_id == s.customer_id, how="inner")
        .join(p, s.id == p.product_id, how="inner")
    )
    return (
        df.select(
        col("order_number"),
        col("order_datetime"),
        col("c.customer_id"),
        col("c.customer_name"),
        concat_ws(", ", col("street"), col("number"), col("c.unit")).alias("address"),
        col("postcode"),
        col("city"),
        col("state"),
        col("tax_id"),
        col("tax_code"),
        col("loyalty_segment"),
        col("id").alias("product_id"),
        col("name").alias("product_name"),
        col("curr").alias("currency"),
        col("sales_price").alias("sales_price"),
        col("qty").alias("units_purchased"),
        round((col("sales_price") * col("qty")),2).alias("total_value")
        )
    )
