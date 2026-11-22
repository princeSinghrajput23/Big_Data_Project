# Databricks notebook source
# MAGIC %md
# MAGIC # 🏥 Healthcare Data Engineering Pipeline
# MAGIC ### National Hospital Directory — Medallion Architecture (Bronze → Silver → Gold)
# MAGIC **Dataset:** `workspace.default.healthcare_raw` volume

# COMMAND ----------

# MAGIC %md ## 🥉 PHASE 2: BRONZE LAYER — Raw Ingestion

# COMMAND ----------

# Read raw CSV from Unity Catalog Volume
df_bronze = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .load("/Volumes/workspace/default/healthcare_raw/national_hospital_directory.csv")

print(f"Total Records: {df_bronze.count()}")
print(f"Total Columns: {len(df_bronze.columns)}")
display(df_bronze.limit(5))

# COMMAND ----------

# Save as Bronze Delta table (all columns as STRING — raw, no changes)
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.hospitals_bronze")

print("✅ Bronze table saved: workspace.default.hospitals_bronze")
spark.sql("SELECT COUNT(*) as total_rows FROM workspace.default.hospitals_bronze").show()

# COMMAND ----------

# MAGIC %md ## 🥈 PHASE 3: SILVER LAYER — Clean & Transform

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("workspace.default.hospitals_bronze")

# Step 1: Replace "0", "NA", empty strings → NULL for all string columns
string_cols = [f.name for f in df.schema.fields]
for col in string_cols:
    df = df.withColumn(col,
        F.when(
            F.col(col).isNull() |
            (F.trim(F.col(col)) == "0") |
            (F.upper(F.trim(F.col(col))) == "NA") |
            (F.trim(F.col(col)) == ""),
            None
        ).otherwise(F.col(col))
    )

print(f"✅ Null handling complete for {len(string_cols)} columns")

# COMMAND ----------

# Step 2: Safe integer casting using try_cast (returns NULL for bad values like phone numbers)
int_cols = [
    "Sr_No", "State_ID", "District_ID",
    "Total_Num_Beds", "Number_Doctor", "Num_Mediconsultant_or_Expert",
    "Number_Private_Wards", "Num_Bed_for_Eco_Weaker_Sec", "Establised_Year"
]
for col in int_cols:
    df = df.withColumn(col, F.expr(f"try_cast(`{col}` as INT)"))

# Replace 0 with NULL for capacity metrics (0 = missing, not actual 0)
for col in ["Total_Num_Beds", "Number_Doctor", "Establised_Year"]:
    df = df.withColumn(col, F.when(F.col(col) == 0, None).otherwise(F.col(col)))

print("✅ Numeric casting complete (using try_cast — bad values become NULL)")
df.select(["Total_Num_Beds", "Number_Doctor", "Establised_Year"]).describe().show()

# COMMAND ----------

# Step 3: Extract Latitude & Longitude from "lat, lon" string
df = df.withColumn(
    "Latitude",
    F.expr("try_cast(split(Location_Coordinates, ',')[0] as DOUBLE)")
).withColumn(
    "Longitude",
    F.expr("try_cast(split(Location_Coordinates, ',')[1] as DOUBLE)")
)

# Step 4: Binary flag columns
df = df.withColumn(
    "Has_Emergency",
    F.when(F.col("Emergency_Services").isNotNull(), 1).otherwise(0)
)
df = df.withColumn(
    "Is_Accredited",
    F.when(F.col("Accreditation").isNotNull(), 1).otherwise(0)
)

# Step 5: Standardize categorical columns
df = df.withColumn(
    "Hospital_Category_Clean",
    F.when(F.col("Hospital_Category").isNull(), "Unknown")
     .otherwise(F.col("Hospital_Category"))
)
df = df.withColumn(
    "Medicine_System",
    F.when(F.col("Discipline_Systems_of_Medicine").isNull(), "Unknown")
     .otherwise(F.col("Discipline_Systems_of_Medicine"))
)

print("✅ Feature engineering complete")
display(df.select("Hospital_Name", "State", "Latitude", "Longitude", "Has_Emergency", "Is_Accredited").limit(10))

# COMMAND ----------

# Step 6: Select final Silver columns and save
df_silver = df.select(
    "Sr_No", "Hospital_Name", "Hospital_Category_Clean",
    "Hospital_Care_Type", "Medicine_System",
    "State", "District", "Subdistrict", "Pincode",
    "Specialties", "Facilities", "Accreditation",
    "Is_Accredited", "Total_Num_Beds", "Number_Doctor",
    "Num_Mediconsultant_or_Expert", "Number_Private_Wards",
    "Emergency_Services", "Has_Emergency", "Tariff_Range",
    "Establised_Year", "Latitude", "Longitude",
    "State_ID", "District_ID",
    "Website", "Ambulance_Phone_No", "Bloodbank_Phone_No"
)

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.hospitals_silver")

count = df_silver.count()
print(f"✅ Silver table saved: workspace.default.hospitals_silver")
print(f"   Total clean records: {count:,}")
display(df_silver.limit(10))

# COMMAND ----------

# MAGIC %md ## 🥇 PHASE 4: GOLD LAYER — Analytics Aggregations

# COMMAND ----------

df_silver = spark.table("workspace.default.hospitals_silver")

# GOLD TABLE 1: State-Level Summary
gold_state = df_silver.groupBy("State", "State_ID").agg(
    F.count("Sr_No").alias("Total_Hospitals"),
    F.sum("Total_Num_Beds").alias("Total_Beds"),
    F.round(F.avg("Total_Num_Beds"), 2).alias("Avg_Beds_Per_Hospital"),
    F.sum("Number_Doctor").alias("Total_Doctors"),
    F.sum("Has_Emergency").alias("Hospitals_With_Emergency"),
    F.sum("Is_Accredited").alias("Accredited_Hospitals"),
    F.round(
        F.sum("Is_Accredited") / F.count("Sr_No") * 100, 2
    ).alias("Accreditation_Rate_Pct")
).orderBy(F.desc("Total_Hospitals"))

gold_state.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_state_summary")

print("✅ gold_state_summary created")
display(gold_state.limit(10))

# COMMAND ----------

# GOLD TABLE 2: Hospital Category KPIs
gold_category = df_silver.groupBy(
    "Hospital_Category_Clean", "Medicine_System"
).agg(
    F.count("Sr_No").alias("Total_Hospitals"),
    F.sum("Total_Num_Beds").alias("Total_Beds"),
    F.sum("Has_Emergency").alias("Emergency_Hospitals"),
    F.sum("Is_Accredited").alias("Accredited_Count"),
    F.round(F.avg("Number_Doctor"), 2).alias("Avg_Doctors")
).orderBy(F.desc("Total_Hospitals"))

gold_category.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_category_summary")

print("✅ gold_category_summary created")
display(gold_category)

# COMMAND ----------

# GOLD TABLE 3: District Heatmap (for Power BI Map visual)
gold_district = df_silver.groupBy(
    "State", "District", "District_ID"
).agg(
    F.count("Sr_No").alias("Total_Hospitals"),
    F.sum("Total_Num_Beds").alias("Total_Beds"),
    F.round(F.avg("Latitude"), 6).alias("Avg_Latitude"),
    F.round(F.avg("Longitude"), 6).alias("Avg_Longitude"),
    F.sum("Has_Emergency").alias("Emergency_Count"),
    F.sum("Is_Accredited").alias("Accredited_Count")
).filter(F.col("Avg_Latitude").isNotNull())

gold_district.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_district_heatmap")

print("✅ gold_district_heatmap created")
display(gold_district.limit(10))

# COMMAND ----------

# GOLD TABLE 4: Hospitals with Specialties (detail drillthrough for Power BI)
gold_specialties = df_silver.filter(
    F.col("Specialties").isNotNull()
).select(
    "Hospital_Name", "State", "District",
    "Total_Num_Beds", "Number_Doctor",
    "Is_Accredited", "Has_Emergency",
    "Hospital_Category_Clean", "Specialties",
    "Latitude", "Longitude"
)

gold_specialties.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("workspace.default.gold_specialties_hospitals")

print(f"✅ gold_specialties_hospitals created: {gold_specialties.count():,} rows")
display(gold_specialties.limit(10))

# COMMAND ----------

# MAGIC %md ## ✅ Final Verification — All Tables

# COMMAND ----------

print("=" * 60)
print("  ALL DELTA TABLES — FINAL ROW COUNTS")
print("=" * 60)
tables = [
    "workspace.default.hospitals_bronze",
    "workspace.default.hospitals_silver",
    "workspace.default.gold_state_summary",
    "workspace.default.gold_category_summary",
    "workspace.default.gold_district_heatmap",
    "workspace.default.gold_specialties_hospitals"
]
for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
    name = table.split(".")[-1]
    print(f"  ✅ {name:<38} {count:>8,} rows")

print("=" * 60)
print("  🎉 Pipeline complete! Ready to connect to Power BI.")
print("=" * 60)
