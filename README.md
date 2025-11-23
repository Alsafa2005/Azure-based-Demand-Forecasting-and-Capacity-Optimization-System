PROJECT ARCHITECTURE :-

![Image](https://github.com/user-attachments/assets/8f37ae26-36ba-47d0-bf93-8578bdf0d079)

---


DEMO VIDEO:-

https://github.com/user-attachments/assets/0fe06bcf-cd8a-4c52-bcfd-c5dc21b9b72f

---


POWER BI IMAGES:-

![Image](https://github.com/user-attachments/assets/09cb024b-2b05-4cc6-8146-428d412b3c30)

![Image](https://github.com/user-attachments/assets/f3657f8b-206f-4c13-bc6b-0884e4603a0a)

![Image](https://github.com/user-attachments/assets/38521f0d-42db-4fe3-aa97-e54bec481f91)

![Image](https://github.com/user-attachments/assets/346530a0-2061-40b3-9874-9e5f2a33668b)

---

# INFOSYS INTERNSHIP 6.0 
This is project file of Azure based Demand Forecasting and Capacity Optimization System of Infosys Springboard 6.0 Internship

---

## 📊 Power BI Dashboard

[👉 Click here to open the Power BI Dashboard](https://app.powerbi.com/view?r=eyJrIjoiOGRhOTVmZDItNThhZC00MWJmLTkxNzUtYWVkYTZkNGM4NzRkIiwidCI6IjI5MTk2MTM0LTRiNzktNDY1NS1hYTZjLTAyNTc2MzQ5NGI2NCJ9)(https://app.powerbi.com/view?r=eyJrIjoiOGRhOTVmZDItNThhZC00MWJmLTkxNzUtYWVkYTZkNGM4NzRkIiwidCI6IjI5MTk2MTM0LTRiNzktNDY1NS1hYTZjLTAyNTc2MzQ5NGI2NCJ9)

---

📌 Project Architecture Overview 

This project implements a complete end-to-end data engineering and analytics pipeline using multiple cloud platforms and Azure services. The architecture integrates diverse data sources, performs ingestion, storage, processing, machine learning, and visualization through a scalable and automated workflow.


---

⚙️ 1. Data Sources

The pipeline collects data from three major sources:

Snowflake Database – Structured data extracted from Snowflake table.

GCP or AWS S3 – Raw or semi-structured file (csv) stored.

Render API – REST API–based data ingestion from the Render-hosted application.


These sources act as the primary data inputs for processing in Azure.


---

🚀 2. Data Ingestion (Azure Data Factory)

Azure Data Factory (ADF) orchestrates the end-to-end ingestion pipeline:

Connects to Snowflake, GCP or AWS S3, and Render API using dedicated connectors.

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



