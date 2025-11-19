# Databricks notebook source
# ================================
#  PATHS FOR SILVER & GOLD
# ================================

silver_desc = "dbfs:/FileStore/tables/silver_desc"
silver_supply = "dbfs:/FileStore/tables/silver_supply"
silver_logs = "dbfs:/FileStore/tables/silver_logs"

gold_features = "dbfs:/FileStore/tables/gold_features"

print("Gold paths ready!")

# COMMAND ----------

# ================================
#  LOAD SILVER DATA
# ================================

df_desc = spark.read.format("delta").load(silver_desc)
df_supply = spark.read.format("delta").load(silver_supply)
df_logs = spark.read.format("delta").load(silver_logs)

print("Silver data loaded!")


# COMMAND ----------

# ================================
#  FIX DATA TYPES
# ================================
# ================================
#  TIME FEATURES FOR FORECASTING (robust version)
# ================================

from pyspark.sql.functions import (
    col, year, month, dayofweek, to_date, to_timestamp, when
)

# 1) Inspect df_logs to see available columns & types
print("Columns in df_logs:", df_logs.columns)
df_logs.printSchema()

# 2) Ensure we have a 'date' column (create if missing)
# Try common timestamp/date column names in order of preference
candidate_cols = ["date", "timestamp", "time", "Date", "created_at"]

found = None
for c in candidate_cols:
    if c in df_logs.columns:
        found = c
        break

if found is None:
    # If none found, raise a helpful error
    raise ValueError(
        "No obvious date/timestamp column found in df_logs. "
        "Expected one of: " + ", ".join(candidate_cols)
    )

print(f"Using column '{found}' to create 'date' column.")

# If the chosen column already looks like a date (date type), cast to date
# If it's a timestamp string, try to parse as timestamp then convert to date
# We'll try safe casting: if it's string and contains a space or 'T' parse as timestamp
from pyspark.sql.functions import length

# Create a temporary column _tmp_ts to hold parsed timestamp if needed
df_logs = df_logs.withColumn("_tmp_orig_type", col(found).cast("string"))

# Try to parse: first try to_timestamp then to_date fallback
# We use to_timestamp then to_date — if already a date, to_date will work.
df_logs = df_logs.withColumn(
    "date",
    when(col(found).cast("timestamp").isNotNull(), to_date(col(found).cast("timestamp")))
    .otherwise(to_date(col(found)))
)

# If resulting 'date' is still null for all rows, try parsing as ISO with to_timestamp
null_count = df_logs.filter(col("date").isNull()).count()
total_count = df_logs.count()
if null_count == total_count:
    # try parsing using generic to_timestamp (ISO)
    df_logs = df_logs.withColumn("date", to_date(to_timestamp(col(found))))
    null_count2 = df_logs.filter(col("date").isNull()).count()
    if null_count2 == total_count:
        print("Warning: unable to parse any dates from column", found)
    else:
        print("Parsed some dates after second attempt.")

# 3) Create time features
df_logs_feat = (
    df_logs
    .withColumn("year", year(col("date")))
    .withColumn("month", month(col("date")))
    .withColumn("weekday", dayofweek(col("date")))
)

print("Time features (year, month, weekday) created. Preview:")
display(df_logs_feat.limit(5))


# COMMAND ----------

# ==========================
#  AGGREGATE METRICS FOR GOLD
# ==========================

from pyspark.sql.functions import col, to_date, year, month, dayofweek

# Convert your string date "09-01-2017" → proper date format (MM-dd-yyyy or dd-MM-yyyy)
# We must detect the format. Based on your screenshot it's "dd-MM-yyyy".

df_logs_fixed = df_logs.withColumn(
    "date_fixed",
    to_date(col("Date"), "dd-MM-yyyy")
)

df_logs_feat = (
    df_logs_fixed
    .withColumn("year", year(col("date_fixed")))
    .withColumn("month_num", month(col("date_fixed")))
    .withColumn("weekday", dayofweek(col("date_fixed")))
)

display(df_logs_feat.limit(10))

# COMMAND ----------

# ============================================
#  DAILY TRAFFIC AGGREGATION
# ============================================

gold_daily = (
    df_logs_feat
    .groupBy("date_fixed", "Category", "Department")
    .count()
    .withColumnRenamed("count", "daily_visits")
)

display(gold_daily.limit(20))

# COMMAND ----------

# ============================================
#  MONTHLY CATEGORY PERFORMANCE
# ============================================

gold_monthly_category = (
    df_logs_feat
    .groupBy("year", "month_num", "Category")
    .count()
    .withColumnRenamed("count", "monthly_category_visits")
    .orderBy("year", "month_num")
)

display(gold_monthly_category.limit(20))

# COMMAND ----------

# ============================================
#  TOP PRODUCTS
# ============================================

gold_top_products = (
    df_logs_feat
    .groupBy("Product")
    .count()
    .withColumnRenamed("count", "total_views")
    .orderBy(col("total_views").desc())
)

display(gold_top_products.limit(10))

# COMMAND ----------

# ============================================
#  SAVE GOLD LAYER
# ============================================

gold_base = "dbfs:/FileStore/tables/gold/"

gold_daily.write.format("delta").mode("overwrite").save(gold_base + "gold_daily")
gold_monthly_category.write.format("delta").mode("overwrite").save(gold_base + "gold_monthly_category")
gold_top_products.write.format("delta").mode("overwrite").save(gold_base + "gold_top_products")

print("Gold tables saved successfully!")

# COMMAND ----------

# =====================================================
#  CREATE & SAVE 90-DAY DEMAND FORECAST
# =====================================================
from pyspark.sql.functions import col
from prophet import Prophet
import pandas as pd

# =====================================
#  Load Silver supply table
# =====================================
silver_supply_path = "dbfs:/FileStore/tables/silver_supply"

df = spark.read.format("delta").load(silver_supply_path)
print("Loaded Silver table:", df.count(), "rows")

# =====================================
#  Set correct date and demand columns
# =====================================
date_col = "order_date_DateOrders"
demand_col = "Order_Item_Quantity"

print("Using date column:", date_col)
print("Using demand column:", demand_col)

# =====================================
#  Prepare data for Prophet
# =====================================

# Convert date + demand to pandas format
pdf = (
    df.select(
        col(date_col).alias("ds"),
        col(demand_col).cast("integer").alias("y")   # ensure integer
    )
    .dropna()
    .toPandas()
)

# Convert ds to datetime
pdf["ds"] = pd.to_datetime(pdf["ds"], errors='coerce')
pdf = pdf.dropna()

print("Prepared data:", pdf.head())

# =====================================
#  Train Prophet model
# =====================================
model = Prophet()
model.fit(pdf)

# =====================================
#  Forecast next 30 days
# =====================================
future = model.make_future_dataframe(periods=30)
forecast = model.predict(future)

print("Forecast completed!")

# =====================================
# 
#  Save forecast to GOLD
# (currently saving in FileStore gold folder)
# =====================================
forecast_spark = spark.createDataFrame(forecast)

gold_forecast_path = "dbfs:/FileStore/tables/gold/forecast_30_days"

forecast_spark.write.format("delta").mode("overwrite").save(gold_forecast_path)

print("Saved forecast to:", gold_forecast_path)


# COMMAND ----------

# ================================================
# Preview Forecast (Using Your Path)
# ================================================

forecast_path = "dbfs:/FileStore/tables/gold/forecast_30_days"

df_forecast = spark.read.format("delta").load(forecast_path)

display(df_forecast)


# COMMAND ----------

spark.conf.set(
    "fs.azure.sas.gold.supplychainforecasting.dfs.core.windows.net",
    "sp=racwl&st=2025-11-18T15:29:57Z&se=2025-11-18T23:44:57Z&sv=2024-11-04&sr=c&sig=4o%2F%2BJMwrMstxPDxW%2BdUgXX%2B05cHOQ5WH75cXGGJe8%2BM%3D"
)


# COMMAND ----------

gold_container_path = "abfss://gold@supplychainforecasting.dfs.core.windows.net/"

forecast_path_final = gold_container_path + "forecast_30_days"
clean_supply_path_final = gold_container_path + "clean_supply"
clean_desc_path_final = gold_container_path + "clean_description"
clean_logs_path_final = gold_container_path + "clean_logs"

# COMMAND ----------

import shutil
import os

# ================
#  GOLD DataFrames
# ================
datasets = {
    "gold_supply": df_supply,
    "gold_description": df_desc,
    "gold_logs": df_logs,
    "gold_forecast_30days": df_forecast
}

print("Detected GOLD DataFrames:", datasets.keys())

# =======================
#  Create local temp folder
# =======================
local_dir = "/tmp/gold_exports"
if os.path.exists(local_dir):
    shutil.rmtree(local_dir)
os.makedirs(local_dir)

# =======================
#  Export each DF as CSV
# =======================
for name, df in datasets.items():
    path = f"{local_dir}/{name}.csv"
    df.toPandas().to_csv(path, index=False)
    print(f" Saved CSV: {path}")

# =======================
#  Create ZIP file
# =======================
zip_path_local = "/tmp/gold_exports.zip"
shutil.make_archive("/tmp/gold_exports", 'zip', local_dir)

print(" Zipped GOLD datasets at:", zip_path_local)

# =======================
#  Move ZIP to DBFS for download
# =======================
dbfs_target = "dbfs:/FileStore/tables/gold_exports.zip"
dbutils.fs.cp("file:" + zip_path_local, dbfs_target)

print("\n🎉 FINAL ZIP AVAILABLE HERE:")
print(dbfs_target)

# Show directory
dbutils.fs.ls("dbfs:/FileStore/tables/")


# COMMAND ----------

# Copy the file from original DBFS location to FileStore
dbutils.fs.cp("dbfs:/FileStore/tables/gold_exports.zip", "dbfs:/FileStore/gold_exports_copy.zip")

# COMMAND ----------

displayHTML('<a href="/files/gold_exports_copy.zip" target="_blank">Click here to download the zip</a>')

# COMMAND ----------

# Save df_logs as Delta table with schema merge
df_logs.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_logs")

df_supply.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_supply")

df_desc.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("Gold_description")

df_forecast.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_forecast_30_days")


# COMMAND ----------

spark.sql("SHOW TABLES IN default").show()


# COMMAND ----------

# Save Gold DataFrames as Delta tables in the default database
df_logs.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_logs")

df_supply.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_supply")

df_desc.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("Gold_description")

df_forecast.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("gold_forecast_30_days")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- In a notebook cell with %sql
# MAGIC CREATE DATABASE IF NOT EXISTS default;
# MAGIC

# COMMAND ----------

# Use Spark to save tables into the SQL catalog
df_logs.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.gold_logs")

df_supply.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.gold_supply")

df_desc.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.Gold_description")

df_forecast.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("default.gold_forecast_30_days")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL cell in Databricks
# MAGIC SHOW TABLES IN default;
# MAGIC