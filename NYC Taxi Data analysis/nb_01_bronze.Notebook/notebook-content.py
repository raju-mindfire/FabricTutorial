# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "warehouse": {
# META       "default_warehouse": "e0e0484e-4ab9-49cc-84d3-1a6e06d4f0f4",
# META       "known_warehouses": [
# META         {
# META           "id": "e0e0484e-4ab9-49cc-84d3-1a6e06d4f0f4",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_01_bronze : Raw data -> Delta table
# **Bronze rule:** keep data as close to the source as possible. No business cleaning here.
# 
# We only:
# - read the parquet file the pipeline downloaded
# - lowercase column names and force a consistent data type (NYC changes types between months!)
# - add audit columns (`source_file`, `ingestion_ts`, `load_ym`)
# - write to Delta using **replaceWhere** so re-running the same month never creates duplicates

# CELL ********************

# MAGIC %%configure -f
# MAGIC {
# MAGIC   "defaultLakehouse": {
# MAGIC     "name": "lh_nyc_taxi"
# MAGIC   }
# MAGIC }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# PARAMETER CELL - values are passed by the pipeline
year  = "2024"
month = "01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

ym = f"{year}-{month}"
file_name = f"yellow_tripdata_{ym}.parquet"
raw_path = f"Files/raw/nyc_taxi/yellow/year={year}/month={month}/{file_name}"
print("Reading:", raw_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 1 - Read the raw parquet file

# CELL ********************

df_raw = spark.read.parquet(raw_path)
df_raw = df_raw.toDF(*[c.lower() for c in df_raw.columns])   # airport_fee vs Airport_fee issue
print("Rows read:", df_raw.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 2 - Standardise data types (same schema for every month)

# CELL ********************

expected_schema = {
    "vendorid": "int", "tpep_pickup_datetime": "timestamp", "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int", "trip_distance": "double", "ratecodeid": "int",
    "store_and_fwd_flag": "string", "pulocationid": "int", "dolocationid": "int",
    "payment_type": "int", "fare_amount": "double", "extra": "double", "mta_tax": "double",
    "tip_amount": "double", "tolls_amount": "double", "improvement_surcharge": "double",
    "total_amount": "double", "congestion_surcharge": "double", "airport_fee": "double"
}

select_cols = []
for col_name, col_type in expected_schema.items():
    if col_name in df_raw.columns:
        select_cols.append(F.col(col_name).cast(col_type).alias(col_name))
    else:
        select_cols.append(F.lit(None).cast(col_type).alias(col_name))   # column missing in this month

df_bronze = df_raw.select(*select_cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 3 - Add audit columns

# CELL ********************

df_bronze = (df_bronze
    .withColumn("source_file",  F.lit(file_name))
    .withColumn("ingestion_ts", F.current_timestamp())
    .withColumn("load_ym",      F.lit(ym)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 4 - Write to Delta (idempotent / re-runnable)

# CELL ********************

table_name = "bronze_yellow_trips"

if spark.catalog.tableExists(table_name):
    (df_bronze.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"load_ym = '{ym}'")   # only replaces THIS month
        .saveAsTable(table_name))
else:
    (df_bronze.write.format("delta")
        .partitionBy("load_ym")
        .saveAsTable(table_name))

print("Bronze loaded for", ym)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 5 - Load the small taxi zone lookup (full refresh, 265 rows)

# CELL ********************

df_zones = (spark.read.option("header", True).option("inferSchema", True)
            .csv("Files/raw/nyc_taxi/lookup/taxi_zone_lookup.csv"))
df_zones = df_zones.toDF(*[c.lower() for c in df_zones.columns])

df_zones.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("bronze_taxi_zones")
display(df_zones.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Check

# CELL ********************

spark.sql("SELECT load_ym, COUNT(*) AS rows FROM bronze_yellow_trips GROUP BY load_ym ORDER BY load_ym").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
