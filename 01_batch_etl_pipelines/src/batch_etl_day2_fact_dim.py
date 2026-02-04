from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("BatchETL-Day2").getOrCreate()

df = spark.read.json("data/input_day2.json")

fact_salary = (
    df
    .withColumn(
        "salary_category",
        when(col("salary") >= 100000, "HIGH").otherwise("LOW")
    )
)

(
    fact_salary
    .write
    .format("delta")
    .mode("append")
    .partitionBy("event_date")
    .save("data/fact_salary")
)

dim_country = (
    df
    .select("country")
    .distinct()
    .withColumn("region", when(col("country") == "IN", "APAC").otherwise("NA"))
    .withColumn("is_active", col("country").isNotNull())
)

(
    dim_country
    .write
    .format("delta")
    .mode("overwrite")
    .save("data/dim_country")
)
