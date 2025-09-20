from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when, concat, upper, lower, substring, to_date, sum as _sum, avg
import os 
import shutil
import glob

spark = SparkSession.builder \
    .appName("ETL_Ventas_Limpieza") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("online_retail_sample.csv", header=True, inferSchema=True)


print("Datos originales:")
df.show(truncate=False)


df_clean = df.withColumn("Description", trim(col("Description"))) \
             .withColumn("Country", trim(col("Country")))

df_clean = df_clean.withColumn(
    "Description",
    when(
        col("Description").isNotNull(),
        concat(
            upper(substring(col("Description"), 1, 1)),
            lower(substring(col("Description"), 2, 100))
        )
    ).otherwise(None)
)

df_clean = df_clean.withColumn(
    "Quantity",
    when(col("Quantity").rlike("^-?[0-9]+$"), col("Quantity").cast("integer")).otherwise(None)
)

df_clean = df_clean.withColumn(
    "UnitPrice",
    when(col("UnitPrice").rlike("^[0-9]+(\\.[0-9]+)?$"), col("UnitPrice").cast("double")).otherwise(None)
)



output_clean_path = "csv/online_retail_class"

if os.path.exists(output_clean_path):
    shutil.rmtree(output_clean_path)
    
df_clean.coalesce(1).write.mode("overwrite").option("header",True).csv(output_clean_path)

part_file = glob.glob(os.path.join(output_clean_path,"part-*.csv"))[0]
final_file = os.path.join("csv","online_retail_clean.csv")
shutil.move(part_file,final_file)

for f in glob.glob(os.path.join(output_clean_path,"*")):
    os.remove(f)
os.rmdir(output_clean_path)

print("Datos limpios:")
df_clean.show(truncate=False)

print(f"Archivo limpio y guardado correctamente en: {final_file}")

spark.stop()
