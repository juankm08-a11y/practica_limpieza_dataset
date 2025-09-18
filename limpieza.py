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
    df.withColumn("Description",trim())
)