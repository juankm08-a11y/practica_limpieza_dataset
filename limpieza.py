from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,trim,when,concat,upper,lower,substring,
    to_date
)

spark = (
    SparkSession.builder \
        .appName("Limpieza Online Retail") \
            .master("local[*]")
            .getOrCreate()
)

df = spark.read.csv("online_retail_sample.csv",header=True,inferSchema=True)

print("---Datos Originales---")
df.show(10,truncate=False)

df_clean = (
    df.withColumn("Description",trim(col("Description"))) \
        .withColumn("Country",trim(col("Country")))
)

df_clean = (
    df_clean.withColumn(
        "Description",
        when(
            col("Description").isNotNull(),
            concat(
                upper(substring(col("Description"),1,1)),
                lower(substring(lower(col("Description")),2,100))
            )
        )
    )
)

df_clean = (
    df_clean.withColumn(
        "Quantity",
        when(col("Quantity").rlike("^-?[0-9]+$"),col("Quantity").cast("integer")).otherwise(None)
    )
)

df_clean = (
    df_clean.withColumn(
        "UnitPrice",
        when(col("UnitPrice").rlike("^-?[0-9]+$"),col("UnitPrice").cast("double")).otherwise(None)
    )
)