# Azure Based Demand Forecasting and Capacity Optimization System

PROJECT ARCHITECTURE :-

![Image](https://github.com/user-attachments/assets/8f37ae26-36ba-47d0-bf93-8578bdf0d079)

---


DEMO VIDEO:-

https://github.com/user-attachments/assets/0fe06bcf-cd8a-4c52-bcfd-c5dc21b9b72f

---

This project implements an end-to-end cloud-based forecasting system designed to predict Azure Compute and Storage demand. The workflow integrates multi-cloud data ingestion, scalable storage, advanced feature engineering, machine learning model training, and Power BI–based visualization to support Azure’s capacity planning and supply chain decision-making.
<img width="1000" height="342" alt="image" src="https://github.com/user-attachments/assets/64ef7ff5-cff4-4b21-813b-b647acbd11e2" />


*1. Data Sources*
   
The solution brings together data from three major platforms:
- Snowflake DB – provides structured enterprise usage and infrastructure data.
- GCP or AWS – supplies external cloud usage signals and demand indicators.
- Render API – provides API-level real-time consumption metrics.
These diverse sources create a rich dataset combining internal and external demand drivers.

<img width="1000" height="342" alt="image" src="https://github.com/user-attachments/assets/6e947ad8-d2bb-4d07-bed2-3da01e5ff3e1" />

---

*2. Data Ingestion Layer*
   
All incoming data is ingested through:

✔ Azure Data Factory (ADF)
+ Automates ingestion workflows
+ Connects securely to Snowflake, GCP, and API endpoints
+ Handles scheduling, pipelines, and logging

✔ Azure Data Lake Storage (ADLS)
+ Acts as the centralized raw data storage location
+ Stores data in hierarchical folders for easy integration with Databricks
This ensures scalable, fault-tolerant data handling capable of growing with Azure’s infrastructure signals.

<img width="1000" height="342" alt="image" src="https://github.com/user-attachments/assets/adca9e84-ada0-4c1e-a29d-d2a84924e34e" />

---

*3. Data Processing Layer (Azure Databricks)*

Azure Databricks is the core data processing and ML environment in this architecture.

✔ Bronze Layer (Raw Data)
+ Stores unprocessed data exactly as received
+ Maintains full data lineage and fidelity

✔ Silver Layer (Clean + Enriched Data)
+ Data cleaning applied
+ Feature engineering performed (lags, moving averages, seasonality, growth, economic indicators, etc.)
+ Time-series alignment and normalization

✔ Gold Layer (Model-Ready Data)
+ Final curated dataset
+ Used for machine learning and dashboard consumption

This multi-layered Lakehouse approach ensures clean, high-quality, reliable input for modeling.

<img width="600" height="442" alt="image" src="https://github.com/user-attachments/assets/ef219e2b-e8fe-44e7-abfb-7310815ae837" />

---

*4. Machine Learning Model Training*

The Model Training block uses Databricks ML runtime to build multiple forecasting models:
+ Random Forest
+ XGBoost Regressor
+ Prophet
+ ARIMA (Auto-ARIMA)

Each model is trained on engineered features from the Silver/Gold layers.

Model	Accuracy-
Random Forest: 	97.69% (Best),
      XGBoost:    97.47%,
        ARIMA:    84.2%,
      Prophet: 	84.19%

Random Forest performed the best due to its ability to learn complex patterns from multiple correlated signals.

The final outputs include:
+ Predicted Compute demand
+ Predicted Storage demand
+ Weekly and monthly trend insights
+ Model performance metrics (MAE, RMSE, SMAPE)

---

*5. Visualization Layer (Power BI)*

The final forecasts and performance metrics are published to Power BI, enabling:
+ Real-time interactive dashboards
+ Demand trend reports
+ Regional and service-level breakdowns
+ Capacity planning insights
+ Model accuracy monitoring

POWER BI DASHBOARD IMAGES:-

![Image](https://github.com/user-attachments/assets/09cb024b-2b05-4cc6-8146-428d412b3c30)

![Image](https://github.com/user-attachments/assets/f3657f8b-206f-4c13-bc6b-0884e4603a0a)

![Image](https://github.com/user-attachments/assets/38521f0d-42db-4fe3-aa97-e54bec481f91)

![Image](https://github.com/user-attachments/assets/346530a0-2061-40b3-9874-9e5f2a33668b)


The dashboard is visible in this link: [https://app.powerbi.com/view?r=eyJrIjoiOGYxYTA5NWMtZTg3NC00Nzk4LThjZjMtNDVlMmE2OTc1ZmI2IiwidCI6IjE1YzM0OWUxLTBjNTUtNDYwOS1iMzNhLWM2MjJkOWU2NjRlYSJ9](https://app.powerbi.com/view?r=eyJrIjoiOGRhOTVmZDItNThhZC00MWJmLTkxNzUtYWVkYTZkNGM4NzRkIiwidCI6IjI5MTk2MTM0LTRiNzktNDY1NS1hYTZjLTAyNTc2MzQ5NGI2NCJ9)

---

*Summary* 

This project delivers an integrated cloud-based forecasting system that ingests multi-cloud data from Snowflake, GCP or AWS, and API sources into Azure storage via ADF, processes it through a Databricks Lakehouse (Bronze-Silver-Gold layers), trains multiple ML models to predict Azure Compute & Storage demand, and visualizes the insights in Power BI.
Random Forest delivered the best forecast accuracy (97.69%), enabling Azure to optimize capacity planning and reduce infrastructure cost inefficiencies.

