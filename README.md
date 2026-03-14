# End-to-End Data Engineering Pipeline 🎧

![Azure](https://img.shields.io/badge/Cloud-Azure-blue)
![Python](https://img.shields.io/badge/Python-3.x-yellow)
![PySpark](https://img.shields.io/badge/Processing-PySpark-orange)
![Databricks](https://img.shields.io/badge/Platform-Databricks-red)
![Data Engineering](https://img.shields.io/badge/Project-Data%20Engineering-green)

An end-to-end **Data Engineering pipeline** built using Azure cloud services to ingest, process, transform, and store Spotify data for analytics.

This project demonstrates how modern data engineering pipelines are designed using **Azure Data Factory, Azure Data Lake Storage, Databricks, and PySpark**.

The project is based on a hands-on tutorial and simulates a real-world data engineering workflow.

---

# 📌 Project Overview

The goal of this project is to build a **scalable data pipeline** that extracts Spotify data, processes it, and loads it into a structured data model for analysis.

The pipeline includes:

• Data ingestion  
• Data storage in data lake  
• Data transformation using PySpark  
• Data modeling  
• Loading processed data for analytics

---

# 🏗️ Architecture
```
The pipeline architecture follows the **Modern Data Engineering Architecture**.

Data Source (Spotify API / Dataset)
↓
Azure Data Factory (Data Ingestion)
↓
Azure Data Lake Storage (Raw Data)
↓
Azure Databricks (Data Processing - PySpark)
↓
Transformed Data (Curated Layer)
↓
Azure SQL Database (Analytics Layer)
```

---

# ⚙️ Technologies Used

| Technology | Purpose |
|------------|--------|
| Azure Data Factory | Data ingestion pipelines |
| Azure Data Lake Storage | Raw & processed data storage |
| Azure Databricks | Data transformation |
| PySpark | Big data processing |
| Python | Data engineering scripts |
| Azure SQL Database | Data warehouse layer |
| GitHub | Version control |

---

## 📂 Project Structure

```
spotify-data-engineering-project
│
├── data
│   ├── raw/                # Raw data ingested from source
│   └── processed/          # Cleaned and transformed datasets
│
├── notebooks
│   └── databricks_transformation/   # Databricks notebooks for data transformation
│
├── pipelines
│   └── adf_pipelines/      # Azure Data Factory pipeline definitions
│
├── scripts
│   └── pyspark_scripts/    # PySpark scripts used for data processing
│
├── sql
│   └── warehouse_tables/   # SQL scripts for creating tables and schema
│
└── README.md               # Project documentation
```


---

# 🔄 Data Pipeline Workflow

### 1️⃣ Data Ingestion
Data is ingested using **Azure Data Factory pipelines** and stored in **Azure Data Lake Storage (Raw Layer)**.

### 2️⃣ Data Processing
Data is processed using **Azure Databricks notebooks with PySpark**.

### 3️⃣ Data Transformation
Transformation includes:

- Cleaning data
- Schema structuring
- Handling missing values
- Creating analytical tables

### 4️⃣ Data Storage
Processed data is stored in **Azure Data Lake curated layer** and loaded into **Azure SQL Database**.

---

# 📊 Data Modeling

The project implements a **Star Schema** for analytics.

### Fact Tables
- fact_tracks
- fact_streams

### Dimension Tables
- dim_artist
- dim_album
- dim_date

This design improves **query performance and analytical capabilities**.

---

# 🚀 How to Run the Project

### 1. Create Azure Resources
- Azure Data Factory
- Azure Data Lake Storage
- Azure Databricks
- Azure SQL Database

### 2. Configure Data Factory Pipelines
Create pipelines to ingest source data into the Data Lake.

### 3. Run Databricks Notebooks
Execute PySpark notebooks to clean and transform the data.

### 4. Load Data to SQL Database
Store processed data for analytics and reporting.

## 📸 Project Screenshots

### Azure Data Factory Pipeline
![ADF Pipeline](screenshots/ADF%20Incremental%20Ingestion%20Pipeline.png)

### Databricks Notebook
![Databricks Notebook](screenshots/Databricks%20Notebook.png)

### Databricks Declarative Pipeline
![Databricks Declarative Pipeline](screenshots/Databricks%20Declarative%20Pipeline.png)

### Azure Storage Account Container
![Azure Storage Account Container](screenshots/Azure%20Storage%20Account%20Container.png)

### Azure Resource Group
![Azure Resource Group](screenshots/Azure%20Resource%20Group.png)

# 🎯 Key Learning Outcomes

✔ Building end-to-end data pipelines  
✔ Working with Azure cloud data services  
✔ Data transformation using PySpark  
✔ Designing scalable data architectures  
✔ Implementing dimensional data models  
✔ Real-world data engineering workflow

