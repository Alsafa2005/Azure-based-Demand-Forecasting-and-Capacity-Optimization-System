# Databricks notebook source
# =========================
#  BRONZE PATHS
# =========================

bronze_desc = "dbfs:/FileStore/tables/bronze_desc"
bronze_supply = "dbfs:/FileStore/tables/bronze_supply"
bronze_logs = "dbfs:/FileStore/tables/bronze_logs"

print("Bronze paths set!")


# COMMAND ----------

# =========================
#  READ RAW CSV FILES
# =========================

df_desc = spark.read.option("header", True).csv("dbfs:/FileStore/tables/DescriptionDataCoSupplyChain_unique.csv")
df_supply = spark.read.option("header", True).csv("dbfs:/FileStore/tables/DataCoSupplyChainDataset_Cleaned_Final_Unique-1.csv")
df_logs = spark.read.option("header", True).csv("dbfs:/FileStore/tables/tokenized_access_logs.csv")

print("Raw CSV files loaded successfully!")


# COMMAND ----------

# =========================
#  SAMPLE RAW DATA
# =========================

display(df_desc)
display(df_supply)
display(df_logs)


# COMMAND ----------

# =========================
#  SAVE AS DELTA → BRONZE
# =========================
# Clean column names to remove invalid characters for Delta Lake
def clean_column_names(df):
    for col in df.columns:
        new_col = (
            col.replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace(",", "")
            .replace(";", "")
            .replace("{", "")
            .replace("}", "")
            .replace("\n", "")
            .replace("\t", "")
            .replace("=", "")
        )
        if new_col != col:
            df = df.withColumnRenamed(col, new_col)
    return df

df_desc = clean_column_names(df_desc)
df_supply = clean_column_names(df_supply)
df_logs = clean_column_names(df_logs)

df_desc.write.format("delta").mode("overwrite").save(bronze_desc)
df_supply.write.format("delta").mode("overwrite").save(bronze_supply)
df_logs.write.format("delta").mode("overwrite").save(bronze_logs)

print(" Bronze layer created successfully!")