from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, to_date

spark = SparkSession.builder \
    .appName("ETL_Online_Retail") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("online_retail_sample.csv", header=True, inferSchema=True)

print("--- Datos para ETL ---")
df.show(10, truncate=False)

ventas_por_pais = df.withColumn("total_price", col("Quantity") * col("UnitPrice")) \
    .groupBy("Country") \
    .agg(_sum("total_price").alias("ventas_totales"))

ticket_promedio = df.groupBy("InvoiceNo") \
    .agg(
        _sum(col("Quantity") * col("UnitPrice")).alias("total_factura")
    ) \
    .agg(avg("total_factura").alias("avg_ticket"))

print("- Ventas totales por país ---")
ventas_por_pais.orderBy("ventas_totales", ascending=False).show(10, truncate=False)

print("--- Ticket promedio por factura ---")
ticket_promedio.show(5, truncate=False)

ventas_por_pais.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ventas_por_pais")
ticket_promedio.coalesce(1).write.mode("overwrite").option("header", True).csv("out/ticket_promedio_factura")

spark.stop()
