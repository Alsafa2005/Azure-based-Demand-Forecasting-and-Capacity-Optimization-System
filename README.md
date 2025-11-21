# INFOSYS INTERNSHIP 6.0 
This is project file of Azure based Demand Forecasting and Capacity Optimization System of Infosys Springboard 6.0 Internship

Architecture of the project:-![Architecture](https://github.com/user-attachments/assets/1492ad47-a4c1-4acd-957e-deafcfb559b1)


📌 Project Overview

This project demonstrates a complete end-to-end Data Engineering pipeline starting from multi-source data ingestion to final business reporting using Tableau. The dataset was sourced from the Kaggle community, which provided three separate CSV files:

1. DataCoSupplyChainDataset.csv


2. Tokenized_access_logs.csv


3. DescriptionDataCoSupplyChainDataset.csv



Each file was intentionally stored on different cloud platforms to simulate real-world heterogeneous data sources.

These files are also present in this git repository in .zip format
---

🚀 Data Ingestion

1. Multi-Cloud Data Storage

DataCoSupplyChainDataset.csv → Uploaded to AWS S3 Bucket

Tokenized_access_logs.csv → Stored in Snowflake

DescriptionDataCoSupplyChainDataset.csv → Exposed through a custom-built API endpoint


This setup represents modern enterprise data scenarios where data is scattered across multiple systems.


---

🔄 Data Movement to ADLS using ADF

Azure Data Factory (ADF) pipelines were created to ingest all three datasets into Azure Data Lake Storage (ADLS).

Pipeline 1: Ingest AWS S3 dataset

Pipeline 2: Ingest Snowflake dataset

Pipeline 3: Call API endpoint and store the response in ADLS


All raw files were stored in the Bronze layer of ADLS following the Medallion Architecture pattern.


---

🧱 Medallion Architecture (Bronze → Silver → Gold)

We implemented the Medallion Architecture inside Databricks to improve data quality, structure, and usability.The notebooks for these layers had been attached above in repositery in .py files.

🔹 Bronze Layer – Raw Ingestion

Direct ingestion of all three datasets from ADLS

No transformations applied

Data stored exactly as received


🔸 Silver Layer – Data Cleaning & Transformation

Removed duplicates and nulls

Fixed data types

Joined datasets

Standardized formats

Applied business rules


🟡 Gold Layer – Business Aggregation

Created analytical tables for ML readiness

We have also used Prophet ML Modelling

Feature engineering

Aggregations and KPIs for visualization

Final curated data ready for reporting


At the end of this stage, clean and structured data was stored in the Gold container.The cleaned data consists of Following files

1. cleaned_data_gold_description.csv

2. cleaned_data_gold_forecast_30days.csv
  
3. cleaned_data_gold_logs.csv
 
4. cleaned_data_gold_supply.csv
 
These files are stored in git repository in .zip format



---

📊 Visualization in Tableau

Tableau was connected directly to Databricks SQL endpoint to build interactive dashboards.
The dashboards include:

Sales analysis

Customer analytics

Supply chain performance

Product trends

Opportunity insights


This allows business users to derive insights from the curated Gold layer data.


---

✅ Final Outcome

✔ Complete end-to-end cloud data engineering pipeline

✔ Multi-cloud ingestion (AWS, Snowflake, API)

✔ Centralized storage in ADLS

✔ Medallion workflow in Databricks

✔ Cleaned datasets ready for analytics

✔ Tableau dashboards for business decision-making

