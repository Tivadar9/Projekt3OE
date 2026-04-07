# Databricks notebook source
# DBTITLE 1,Cell 1
# JSON fájl betöltése
import pandas as pd

file_path = "/Workspace/Users/tormasi.tivadar@stud.uni-obuda.hu/Drafts/car_sales_with_faults.json"

# Read JSON with pandas
df_pandas = pd.read_json(file_path)

# Normalize mixed-type columns before converting to Spark
df_pandas['is_new'] = df_pandas['is_new'].astype(str).str.lower().map({'true': True, 'false': False})
df_pandas['has_warranty'] = df_pandas['has_warranty'].astype(str).str.lower().map({'yes': True, 'no': False, '1': True, '0': False, 'true': True, 'false': False})
df_pandas['dealer_id'] = df_pandas['dealer_id'].astype(str).str.replace('D-', '', regex=False).astype(int)

# Create Spark DataFrame from pandas
df_raw = spark.createDataFrame(df_pandas)

display(df_raw)
df_raw.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, lower, trim
from pyspark.sql.types import IntegerType, DoubleType, BooleanType

df_fixed = (
    df_raw
    .withColumn("status", lower(trim(col("status"))))
    .withColumn("price_eur", col("price_eur").cast(DoubleType()))
    .withColumn("year", col("year").cast(IntegerType()))
    .withColumn("mileage_km", col("mileage_km").cast(IntegerType()))
    .withColumn("sale_id", col("sale_id").cast(IntegerType()))
    .withColumn("customer_id", col("customer_id").cast(IntegerType()))
    .withColumn("dealer_id", col("dealer_id").cast(IntegerType()))
    .withColumn("discount_eur", col("discount_eur").cast(DoubleType()))
    .withColumn("final_price_eur", col("final_price_eur").cast(DoubleType()))
    .withColumn("is_new", col("is_new").cast(BooleanType()))
    .withColumn("has_warranty", col("has_warranty").cast(BooleanType()))
)

display(df_fixed)
df_fixed.printSchema()

# COMMAND ----------

display(df_fixed.select("status").distinct())

# COMMAND ----------

from pyspark.sql.functions import sum, when

display(
    df_fixed.select(
        sum(when(col("price_eur").isNull(), 1).otherwise(0)).alias("null_price_eur"),
        sum(when(col("year").isNull(), 1).otherwise(0)).alias("null_year"),
        sum(when(col("mileage_km").isNull(), 1).otherwise(0)).alias("null_mileage_km"),
        sum(when(col("dealer_id").isNull(), 1).otherwise(0)).alias("null_dealer_id"),
        sum(when(col("is_new").isNull(), 1).otherwise(0)).alias("null_is_new"),
        sum(when(col("has_warranty").isNull(), 1).otherwise(0)).alias("null_has_warranty")
    )
)

# COMMAND ----------

table_name = "car_sales_cleaned"

df_fixed.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM car_sales_cleaned
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE car_sales_cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count
# MAGIC FROM car_sales_cleaned;