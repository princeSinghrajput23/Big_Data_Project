# 🏥 Healthcare – Hospital Operations & Capacity Intelligence
### End-to-End Big Data Engineering Project (Databricks $\rightarrow$ Delta Lake $\rightarrow$ Power BI)

[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Serverless_Runtime-red.svg)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion_Architecture-blue.svg)](https://delta.io/)
[![Power BI](https://img.shields.io/badge/Power_BI-Interactive_Dashboard-yellow.svg)](https://powerbi.microsoft.com/)

---

## 📌 1. Project Title & Problem Statement

* **Project Title:** Healthcare Hospital Operations Analytics & Infrastructure Intelligence
* **Domain:** Healthcare Data Engineering (Project 5 from Industry Project Specifications)
* **Problem Statement:**  
  Healthcare administrative bodies, emergency services, and hospital management face substantial challenges in monitoring regional hospital readiness, bed occupancy capacity, doctor-to-bed ratios, and emergency care availability across different districts and states. Unstandardized data formats, corrupt capacity figures, missing geographical identifiers, and isolated data silos prevent real-time operational decision-making.
  
  **Objective:** Build an end-to-end scalable Lakehouse data pipeline implementing the **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** using **PySpark** and **Delta Lake** on **Databricks**, delivering production-ready aggregated tables to **Power BI** for executive hospital operational monitoring.

---

## 🏗️ 2. Project Architecture Diagram

```mermaid
flowchart TD
    subgraph Layer1["Layer 1: Data Ingestion & Storage"]
        RawCSV["📄 national_hospital_directory.csv\n(National Health Dataset)"]
        UCVolume["☁️ Databricks Unity Catalog Volume\n/Volumes/workspace/default/healthcare_raw/"]
        RawCSV -->|CLI Ingestion| UCVolume
    end

    subgraph Layer2["Layer 2: Data Engineering (Medallion Architecture)"]
        BronzeTable["🥉 Bronze Layer (Raw Delta Table)\nworkspace.default.hospitals_bronze\n• Preserves raw data as-is\n• String-safe schema"]
        SilverTable["🥈 Silver Layer (Cleaned & Enriched)\nworkspace.default.hospitals_silver\n• Null normalization (0/NA -> NULL)\n• Safe type casting (try_cast for phone strings)\n• Feature engineering (Lat/Long, Has_Emergency, Is_Accredited)"]
        
        Gold1["🥇 Gold 1: gold_state_summary\n(Hospitals, Beds, Doctors, Accreditation Rate by State)"]
        Gold2["🥇 Gold 2: gold_category_summary\n(Public vs Private vs Ayush Metrics)"]
        Gold3["🥇 Gold 3: gold_district_heatmap\n(Geo Lat/Long coordinates for map visuals)"]
        Gold4["🥇 Gold 4: gold_specialties_hospitals\n(Facility & department drill-down table)"]

        UCVolume -->|PySpark Ingestion| BronzeTable
        BronzeTable -->|Data Cleansing & Validation| SilverTable
        SilverTable -->|Aggregations & Business KPIs| Gold1
        SilverTable -->|Aggregations & Business KPIs| Gold2
        SilverTable -->|Spatial Filtering| Gold3
        SilverTable -->|Department Filtering| Gold4
    end

    subgraph Layer3["Layer 3: Visualization & Business Intelligence"]
        Warehouse["⚡ Databricks Serverless SQL Warehouse\n(Port 443 / JDBC / ODBC)"]
        PowerBI["📊 Power BI Dashboard\n• Executive KPIs\n• State Healthcare Infrastructure\n• Geographic Geo-Bubble Density Map\n• Accreditation & Emergency Coverage"]
        
        Gold1 --> Warehouse
        Gold2 --> Warehouse
        Gold3 --> Warehouse
        Gold4 --> Warehouse
        Warehouse -->|Secure PAT / DirectQuery| PowerBI
    end
```

---

## ⚙️ 3. Tech Stack & Environment

| Component | Technology | Version / Configuration |
|---|---|---|
| **Cloud Lakehouse** | Databricks on AWS | Serverless Runtime |
| **Catalog Governance** | Unity Catalog | `workspace.default` |
| **Compute Engine** | Apache Spark / PySpark | Spark 3.5+ |
| **Storage Format** | Delta Lake (ACID Transactions) | Time Travel & Schema Evolution |
| **Orchestration / Notebook** | Databricks Notebook | Python / PySpark (`Healthcare_ETL_Pipeline`) |
| **BI & Analytics** | Power BI Service & Desktop | DirectQuery / Import Mode |
| **CLI / Automation** | Databricks CLI | v1.26.0 |

---

## 📊 4. Deliverables Progress Tracker (Against Guidelines)

| # | Guideline Deliverable | Status | Description |
|---|---|:---:|---|
| **1** | **Project Title & Real-World Problem Statement** | ✅ **Done** | Documented above covering hospital operations, capacity, and regional care distribution. |
| **2** | **Data Source Details & Ingestion Script** | ✅ **Done** | `national_hospital_directory.csv` (7.4 MB, comprehensive hospital directory covering all Indian states). Ingested via CLI into Unity Catalog Managed Volume. |
| **3** | **Raw Data Storage Implementation** | ✅ **Done** | Implemented at `/Volumes/workspace/default/healthcare_raw/national_hospital_directory.csv`. |
| **4** | **Databricks Notebooks (Bronze, Silver, Gold)** | ✅ **Done** | Created, uploaded, and verified at `/Users/swastikcs25@gmail.com/Healthcare_ETL_Pipeline`. |
| **5** | **Data Cleaning, Transformation & Validation Logic** | ✅ **Done** | PySpark null handling, `try_cast` to resolve malformed strings (e.g. phone numbers in capacity fields), GPS coordinate parsing, binary flags. |
| **6** | **Complete ETL Pipeline Execution** | ✅ **Done** | Executed successfully; all 6 Delta tables generated and verified in Databricks. |
| **7** | **Final Tableau or Power BI Dashboard** | 🟡 **In Progress** | Power BI connection details & credentials generated. Report layout & visual design pending. |
| **8** | **Project Architecture Diagram** | ✅ **Done** | Detailed Mermaid architectural diagram included in README and project docs. |
| **9** | **GitHub Repository & README Documentation** | 🟡 **In Progress** | Comprehensive `README.md` created. Git commit and push to remote repository pending. |
| **10** | **Final Project Report and Presentation** | ⏳ **Pending** | Slide deck and Word/PDF executive report with pipeline execution screenshots. |

---

## 📋 5. Detailed Work Completed

### Phase 1: Environment & Raw Storage Setup
* Configured Databricks CLI v1.26.0 authenticated with workspace `https://dbc-a4a09dbc-2715.cloud.databricks.com`.
* Created Unity Catalog managed volume: `workspace.default.healthcare_raw`.
* Transferred `national_hospital_directory.csv` into Databricks cloud object storage.

### Phase 2: Bronze Layer (Raw Ingestion)
* Ingested CSV using Apache Spark with `multiLine=true` and `escape="\""`.
* Persisted raw records into `workspace.default.hospitals_bronze` as a Delta Lake table with ACID transaction log.

### Phase 3: Silver Layer (Cleansing & Feature Engineering)
* **Null Standardization:** Normalized `"0"`, `"NA"`, `"na"`, and whitespace strings to SQL `NULL`.
* **Safe Type Casting:** Implemented PySpark `try_cast(col AS INT)` across capacity metrics to prevent overflow errors caused by contact numbers mistakenly recorded in bed/doctor count columns.
* **Geospatial Feature Extraction:** Decomposed `Location_Coordinates` into numeric `Latitude` and `Longitude` fields for GIS mapping.
* **Categorical Normalization:** Cleaned system of medicine (Allopathic, Ayurvedic, Homeopathic) and hospital ownership classification.
* **Binary Indicators:** Engineered `Has_Emergency` (1/0) and `Is_Accredited` (1/0) flags.
* Saved to Delta table: `workspace.default.hospitals_silver`.

### Phase 4: Gold Layer (Analytics Modeling)
Built 4 purpose-built analytical Delta tables:
1. **`gold_state_summary`**: State-level rollups for total hospitals, beds, doctors, emergency availability, and accreditation percentages.
2. **`gold_category_summary`**: Segmented KPIs by hospital ownership and medicinal discipline.
3. **`gold_district_heatmap`**: District-level geospatial coordinates and hospital density for geo bubble maps.
4. **`gold_specialties_hospitals`**: Detailed specialty and facility records for drill-down reporting.

### Phase 5: SQL Warehouse & Token Configuration
* Provisioned and started Serverless SQL Warehouse (`9fb4d0b3432ba262`).
* Generated dedicated 90-day Personal Access Token (PAT) for Power BI authentication.

---

## ⏳ 6. What is Pending & Next Steps

```mermaid
flowchart LR
    Task1["📊 Step 1: Connect Power BI\n(Desktop or Web)"] --> Task2["🎨 Step 2: Build Visuals & DAX\n(Cards, Maps, Bar Charts)"]
    Task2 --> Task3["📸 Step 3: Capture Screenshots\n(Pipeline + Dashboard)"]
    Task3 --> Task4["📄 Step 4: Final Report & PPT\n(Submission Deliverable)"]
    Task4 --> Task5["🐙 Step 5: Git Push\n(GitHub Submission)"]
```

### Action Items Checklist:
1. **Power BI Dashboard Creation:**
   - Connect Power BI Desktop or Service to Databricks Gold tables.
   - Build 4 primary dashboard views:
     - *Executive Summary:* National KPIs (Total Hospitals, Total Beds, Total Doctors).
     - *Geographic View:* Bubble Map of hospital density across states and districts.
     - *Capacity & Quality Analysis:* Bed availability per state, accreditation rate gauge.
     - *Emergency Care Readiness:* Ratio of hospitals equipped with 24/7 emergency & blood banks.
2. **DAX Measures Formulation:**
   - Implement `Emergency Coverage %`, `Accreditation %`, and `Doctor-to-Bed Ratio`.
3. **Documentation & Screenshots:**
   - Take screenshots of the running Databricks notebook showing cell executions.
   - Take screenshots of the final Power BI dashboard with interactive slicers.
4. **Final Deliverables Assembly:**
   - Push code to team GitHub repository with this `README.md`.
   - Prepare final project report and presentation slides (due November 20, 2026).

---

## 🔑 7. Power BI Connection Reference

```text
Server Hostname: dbc-a4a09dbc-2715.cloud.databricks.com
HTTP Path:       /sql/1.0/warehouses/9fb4d0b3432ba262
Port:            443
Auth Scheme:     Personal Access Token
Token:           dapi6f4a04e3ea894ffb021f7165a2a2f3eb
Catalog/Schema:  workspace.default
Gold Tables:     gold_state_summary, gold_category_summary,
                 gold_district_heatmap, gold_specialties_hospitals
```

---

## 👥 Team & Submission Guidelines Reference
* **Project Track:** Track 5 — Healthcare Operations Analytics
* **Architecture:** 3-Tier (Extraction $\rightarrow$ Data Engineering $\rightarrow$ Visualization)
* **Deadline:** 20th November 2026
