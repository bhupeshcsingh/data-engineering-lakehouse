from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, when

spark = (
    SparkSession.builder
    .appName("BatchETL-Day1")
    .getOrCreate()
)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("salary", IntegerType(), True)
])

raw_df = spark.read.schema(schema).json("data/input.json")

clean_df = (
    raw_df
    .filter(col("id").isNotNull())
    .withColumn(
        "salary_category",
        when(col("salary") >= 100000, "HIGH").otherwise("LOW")
    )
)

(
    clean_df
    .write
    .mode("overwrite")
    .partitionBy("country")
    .parquet("data/output/")
)
