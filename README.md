# INFOSYS INTERNSHIP 6.0 
This is project file of Azure based Demand Forecasting and Capacity Optimization System of Infosys Springboard 6.0 Internship

ARCHITECTURE:-

![Image](https://github.com/user-attachments/assets/8f37ae26-36ba-47d0-bf93-8578bdf0d079)

---

📌 Project Architecture Overview

This project implements a complete end-to-end data engineering and analytics pipeline using multiple cloud platforms and Azure services. The architecture integrates diverse data sources, performs ingestion, storage, processing, machine learning, and visualization through a scalable and automated workflow.


---

⚙️ 1. Data Sources

The pipeline collects data from three major sources:

Snowflake Database – Structured data extracted from Snowflake table.

AWS S3 – Raw or semi-structured file (csv) stored in Amazon S3 bucket.

Render API – REST API–based data ingestion from the Render-hosted application.


These sources act as the primary data inputs for processing in Azure.


---

🚀 2. Data Ingestion (Azure Data Factory)

Azure Data Factory (ADF) orchestrates the end-to-end ingestion pipeline:

Connects to Snowflake, AWS S3, and Render API using dedicated connectors.

Pulls data on scheduled, triggered, or event-driven processes.

Loads raw data into Azure Data Lake Storage (ADLS).


ADF ensures scalable, secure, and automated ingestion from all sources.


---

🗄️ 3. Data Storage (Azure Data Lake Storage - ADLS)

All ingested data is stored in ADLS, which acts as the centralized storage layer:

Raw zone (Bronze) – Holds unmodified original data.

Clean zone (Silver) – Stores cleaned and transformed datasets.

Curated zone (Gold) – Contains enriched, aggregated, business-ready data.


ADLS enables scalable, low-cost storage for both raw and processed data.


---

🔧 4. Data Processing (Azure Databricks with Medallion Architecture)

Azure Databricks processes data using the Medallion Architecture, ensuring quality and structure at every stage:

Bronze Layer → Raw ingested data.

Silver Layer → Cleaned, standardized, deduplicated data.

Gold Layer → Aggregated and analytics-ready datasets.


Databricks notebooks/workflows handle all ETL and transformation logic.


---

🤖 5. Machine Learning (Model Training)

Gold-layer curated datasets are used to train ML models within Databricks:

Perform feature engineering and dataset preparation.

Train, validate, and evaluate machine learning models.

Generate predictions or deployable model artifacts.


This integrates seamlessly with the Databricks ML ecosystem.


---

📊 6. Data Visualization (Power BI)

Processed and curated Gold datasets are connected to Power BI for analytics and reporting:

Interactive dashboards

Real-time insights

KPI-based business reports


Power BI enables stakeholders to consume insights derived from the pipeline.


---

🧩 End-to-End Workflow Summary

1. Extract data from Snowflake, AWS S3, and Render API via ADF.


2. Load raw data into Azure Data Lake Storage.


3. Transform using Databricks’ Bronze → Silver → Gold processing.


4. Train ML Models on curated Gold-layer data.


5. Visualize final outputs using Power BI dashboards.



