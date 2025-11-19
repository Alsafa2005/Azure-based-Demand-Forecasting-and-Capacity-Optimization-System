# Databricks notebook source
# =========================
# PATHS FOR BRONZE & SILVER
# =========================

bronze_desc = "dbfs:/FileStore/tables/bronze_desc"
bronze_supply = "dbfs:/FileStore/tables/bronze_supply"
bronze_logs = "dbfs:/FileStore/tables/bronze_logs"

silver_desc = "dbfs:/FileStore/tables/silver_desc"
silver_supply = "dbfs:/FileStore/tables/silver_supply"
silver_logs = "dbfs:/FileStore/tables/silver_logs"

print("Silver paths ready!")


# COMMAND ----------

# =========================
# LOAD BRONZE DATA
# =========================

df_desc = spark.read.format("delta").load(bronze_desc)
df_supply = spark.read.format("delta").load(bronze_supply)
df_logs = spark.read.format("delta").load(bronze_logs)

print("Bronze data loaded!")


# COMMAND ----------

# =========================
# CLEANING OPERATIONS
# =========================

from pyspark.sql.functions import col, trim

# Trim extra spaces
df_desc_clean = df_desc.select([trim(col(c)).alias(c) for c in df_desc.columns])

# Remove duplicates + fill missing values
df_supply_clean = df_supply.dropDuplicates().fillna("Unknown")

# Clean logs
df_logs_clean = df_logs.dropDuplicates()

print("Data cleaning completed!")



# COMMAND ----------

# =========================
#  PREVIEW CLEANED DATA
# =========================

display(df_desc_clean)
display(df_supply_clean)
display(df_logs_clean)


# COMMAND ----------

# =========================
# SAVE SILVER DELTA TABLES
# =========================

df_desc_clean.write.format("delta").mode("overwrite").save(silver_desc)
df_supply_clean.write.format("delta").mode("overwrite").save(silver_supply)
df_logs_clean.write.format("delta").mode("overwrite").save(silver_logs)

print(" Silver layer created successfully!")
