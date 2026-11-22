# Databricks Lakeflow Declarative ETL Pipeline
# Project: Healthcare Hospital Operations Analytics (Track 5)
# Architecture: Medallion (Bronze -> Silver -> Gold)

import dlt
from pyspark.sql import functions as F

# =========================================================================
# 🥉 BRONZE LAYER: Raw Data Ingestion from Unity Catalog Volume
# =========================================================================
@dlt.table(
    name="dlt_hospitals_bronze",
    comment="Raw ingested records from National Hospital Directory CSV"
)
def dlt_hospitals_bronze():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("escape", '"')
        .load("/Volumes/workspace/default/healthcare_raw/national_hospital_directory.csv")
    )

# =========================================================================
# 🥈 SILVER LAYER: Data Cleansing, Safe Casting & Feature Engineering
# =========================================================================
@dlt.table(
    name="dlt_hospitals_silver",
    comment="Cleaned, normalized, and GIS-enriched healthcare records"
)
@dlt.expect_or_drop("valid_hospital", "Hospital_Name IS NOT NULL")
def dlt_hospitals_silver():
    df = dlt.read("dlt_hospitals_bronze")
    
    # 1. Null Standardization (0, NA, empty string -> NULL)
    for col_name in df.columns:
        df = df.withColumn(
            col_name,
            F.when(
                (F.col(col_name).isNull()) |
                (F.trim(F.col(col_name)) == "0") |
                (F.upper(F.trim(F.col(col_name))) == "NA") |
                (F.trim(F.col(col_name)) == ""),
                None
            ).otherwise(F.col(col_name))
        )
    
    # 2. Safe Integer Casting (avoid overflow from corrupt 10-digit phone strings)
    for num_col in ["Total_Num_Beds", "Number_Doctor", "Establised_Year", "State_ID", "District_ID"]:
        df = df.withColumn(num_col, F.expr(f"try_cast(`{num_col}` as INT)"))
    
    # 3. Geospatial extraction (split lat, lon)
    df = df.withColumn("Latitude", F.expr("try_cast(split(Location_Coordinates, ',')[0] as DOUBLE)"))
    df = df.withColumn("Longitude", F.expr("try_cast(split(Location_Coordinates, ',')[1] as DOUBLE)"))
    
    # 4. Binary indicators
    df = df.withColumn("Has_Emergency", F.when(F.col("Emergency_Services").isNotNull(), 1).otherwise(0))
    df = df.withColumn("Is_Accredited", F.when(F.col("Accreditation").isNotNull(), 1).otherwise(0))
    
    # 5. Ownership categorization
    df = df.withColumn(
        "Hospital_Category_Clean",
        F.when(F.col("Hospital_Category").isNull(), "Unknown").otherwise(F.col("Hospital_Category"))
    )
    
    return df

# =========================================================================
# 🥇 GOLD LAYER: Business Analytical Models & Reporting Aggregates
# =========================================================================
@dlt.table(
    name="dlt_gold_state_summary",
    comment="Gold aggregation: State-level hospital capacity, beds, and doctors"
)
def dlt_gold_state_summary():
    return (
        dlt.read("dlt_hospitals_silver")
        .groupBy("State", "State_ID")
        .agg(
            F.count("Sr_No").alias("Total_Hospitals"),
            F.sum("Total_Num_Beds").alias("Total_Beds"),
            F.round(F.avg("Total_Num_Beds"), 2).alias("Avg_Beds_Per_Hospital"),
            F.sum("Number_Doctor").alias("Total_Doctors"),
            F.sum("Has_Emergency").alias("Hospitals_With_Emergency"),
            F.sum("Is_Accredited").alias("Accredited_Hospitals"),
            F.round(F.sum("Is_Accredited") / F.count("Sr_No") * 100, 2).alias("Accreditation_Rate_Pct")
        )
    )

@dlt.table(
    name="dlt_gold_category_summary",
    comment="Gold aggregation: Public vs Private vs Ayush facility metrics"
)
def dlt_gold_category_summary():
    return (
        dlt.read("dlt_hospitals_silver")
        .groupBy("Hospital_Category_Clean")
        .agg(
            F.count("Sr_No").alias("Total_Hospitals"),
            F.sum("Total_Num_Beds").alias("Total_Beds"),
            F.sum("Has_Emergency").alias("Emergency_Hospitals"),
            F.round(F.avg("Number_Doctor"), 2).alias("Avg_Doctors")
        )
    )
