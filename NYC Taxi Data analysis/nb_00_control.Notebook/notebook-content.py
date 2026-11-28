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

# <u></u># nb_00_control : Incremental load controller
# **Purpose:** decide *which month* the pipeline should load next.
# 
# **How incremental loading works here**
# 1. A small Delta table `ctl_load_watermark` remembers the last month loaded (e.g. `2024-03`).
# 2. Every pipeline run asks this notebook: "what is the next month?"
# 3. If next month is beyond `end_ym`, nothing to load -> pipeline stops gracefully.
# 4. The notebook returns a JSON string to the pipeline (via `notebookutils.notebook.exit`).
# 
# > Attach your Lakehouse `lh_nyc_taxi` as the **default lakehouse** before running.

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

# PARAMETER CELL (toggle "parameter cell" in Fabric UI if not auto-detected)
start_ym = "2024-01"   # first month to load when watermark table is empty
end_ym   = "2024-06"   # last month we want to load (raise this later to load more)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 1 - Create the watermark table (only the first time)

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ctl_load_watermark (
    dataset        STRING,
    last_loaded_ym STRING,
    updated_at     TIMESTAMP
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 2 - Read the last loaded month

# CELL ********************

last_ym = spark.sql("""
    SELECT MAX(last_loaded_ym) AS ym
    FROM ctl_load_watermark
    WHERE dataset = 'yellow_taxi'
""").collect()[0]["ym"]

print("Last loaded month:", last_ym)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 3 - Work out the next month

# CELL ********************

def next_month(ym):
    y, m = int(ym[:4]), int(ym[5:7])
    return (y + 1, 1) if m == 12 else (y, m + 1)

if last_ym is None:
    year, month = int(start_ym[:4]), int(start_ym[5:7])   # very first run
else:
    year, month = next_month(last_ym)

month_str = str(month).zfill(2)          # 1 -> "01"
ym = f"{year}-{month_str}"               # "2024-01"
has_new_data = ym <= end_ym              # string compare works for YYYY-MM

print("Next month to load:", ym, "| has_new_data =", has_new_data)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 4 - Return values to the pipeline

# CELL ********************

import json

result = {
    "has_new_data": has_new_data,
    "year": str(year),
    "month": month_str,
    "ym": ym,
    "file_name": f"yellow_tripdata_{ym}.parquet",
    "relative_url": f"trip-data/yellow_tripdata_{ym}.parquet",
    "raw_folder": f"raw/nyc_taxi/yellow/year={year}/month={month_str}"
}

notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
