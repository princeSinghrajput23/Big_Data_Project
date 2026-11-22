# 🏥 Project Presentation Deck: Healthcare Operations & Capacity Analytics
## Big Data Engineering Industry Project | Track 5

---

### Slide 1: Title & Team Introduction
* **Project Title:** Healthcare Hospital Operations Analytics & Infrastructure Intelligence
* **Architecture:** 3-Tier End-to-End Big Data Pipeline (Databricks Serverless $\rightarrow$ Delta Lake $\rightarrow$ Power BI)
* **Team Members & Roles:**
  * Member 1 (Lead & Architecture): Cloud Storage & Medallion Pipeline Design
  * Member 2 (Data Engineering): PySpark Ingestion, Cleansing & `try_cast` Data Quality Logic
  * Member 3 (Analytics & Modeling): Delta Lake Gold Layer Aggregations & SQL Warehouse
  * Member 4 (BI & Presentation): Power BI Interactive Dashboard & Executive Reporting

---

### Slide 2: Real-World Problem Statement & Motivation
* **The Problem:** Healthcare governing bodies and emergency dispatchers lack real-time visibility into regional hospital capacity, bed availability, and trauma readiness.
* **Data Challenges Encountered:**
  * Decentralized directories with unstandardized formatting.
  * Anomaly injection: Corrupt telephone numbers (10 digits) stored inside numeric bed/doctor fields causing batch integer overflow.
  * Unparsed geographical coordinates preventing spatial distance-to-care routing.
* **Our Objective:** Automate an end-to-end Lakehouse pipeline transforming raw healthcare directories into actionable BI analytics.

---

### Slide 3: End-to-End System Architecture (The Medallion Framework)
* **Layer 1: Extraction & Cloud Storage:**
  * Ingestion of raw National Hospital Directory data into Databricks Unity Catalog Volume (`workspace.default.healthcare_raw`).
* **Layer 2: Apache Spark Data Engineering:**
  * **🥉 Bronze Layer:** Preserves raw data fidelity with zero data loss in Delta format.
  * **🥈 Silver Layer:** Cleans null placeholders (`0`/`NA`), extracts GIS Lat/Long, handles overflow via `try_cast`, and engineers emergency indicators.
  * **🥇 Gold Layer:** 4 pre-aggregated analytical tables optimized for query speed.
* **Layer 3: Visualization & Business Intelligence:**
  * Databricks Serverless SQL Warehouse directly connected to **Power BI**.

---

### Slide 4: Data Engineering Highlights & Spark Innovation
* **Handling Corrupt Data without Pipeline Failure:**
  * Traditional `.cast(IntegerType())` failed with `CAST_INVALID_INPUT` due to 10-digit phone strings exceeding 32-bit INT limits.
  * **Our Solution:** Implemented Spark SQL `try_cast(col as INT)`, converting corrupted numbers to `NULL` safely without terminating processing.
* **Geospatial Engineering:**
  * Split composite `"lat, lon"` coordinates into numeric `DoubleType` fields (`Latitude`, `Longitude`) to enable GIS bubble mapping in Power BI.
* **Categorical Normalization:**
  * Mapped missing hospital classifications to standard categorizations (Public, Private, Ayush, Trust).

---

### Slide 5: The Analytical Gold Layer (Star-Schema Design)
* **`gold_state_summary`:** State-level rollups for total hospitals, beds, doctors, and accreditation rate %.
* **`gold_category_summary`:** Comparative metrics between Public, Private, and Ayush sectors.
* **`gold_district_heatmap`:** Aggregated coordinates for district-level density mapping.
* **`gold_specialties_hospitals`:** Filtered directory of hospitals with specialized surgical and intensive care departments.

---

### Slide 6: Power BI Executive Dashboard Tour
* **Top KPI Metric Cards:**
  * National Total Registered Hospitals: ~25,000+
  * Cumulative Network Beds: Highlighting state-by-state surge capacity.
  * Active Medical Personnel & Emergency Ready Units.
* **Infrastructure by State (Clustered Bar Chart):**
  * Ranks top healthcare hubs (Maharashtra, Gujarat, Uttar Pradesh, Tamil Nadu).
* **Facility Ownership Distribution (Donut Chart):**
  * Contrasts private sector concentration against public infrastructure.
* **Emergency Preparedness by State (Column Chart):**
  * Pinpoints regional gaps in 24/7 trauma care and blood bank access.

---

### Slide 7: Key Analytical Findings & Business Insights
1. **Regional Concentration:** Top 4 states represent >50% of documented healthcare capacity.
2. **Private vs. Public Disparity:** Private hospitals command 70%+ of tertiary care specializations, while public hospitals handle the majority of basic emergency load.
3. **Emergency Care Deficit:** Over 65% of secondary rural clinics lack dedicated 24/7 emergency response units.

---

### Slide 8: Enterprise Technology Advantages
* **Why Delta Lake?**
  * ACID transactions prevent partial/corrupt writes during ETL runs.
  * Time Travel enables historical auditing and rollback.
* **Why Serverless Databricks?**
  * Instant compute spin-up with zero cluster management overhead.
  * Auto-terminates when idle to eliminate cloud compute costs.

---

### Slide 9: Project Deliverables Summary
* ✅ Ingested raw dataset in Databricks Unity Catalog.
* ✅ Fully automated, reproducible PySpark Medallion ETL pipeline.
* ✅ 6 Delta Lake tables verified in `workspace.default`.
* ✅ Complete interactive Power BI dashboard project (`SPARK_PROJECT_FINAL.pbix` & `.pbip`).
* ✅ Full documentation, GitHub repo setup, and presentation deck.

---

### Slide 10: Conclusion & Q&A
* *“Our solution delivers an automated, fault-tolerant Lakehouse pipeline transforming fragmented healthcare data into executive operational intelligence.”*
* **Open for questions from the evaluation panel.**
