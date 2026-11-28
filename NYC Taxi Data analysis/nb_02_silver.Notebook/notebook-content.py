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

# # nb_02_silver : Clean, validate, enrich
# **Silver rule:** trustworthy data. Cleaned, typed, de-duplicated, enriched.
# 
# Steps
# 1. Read only the current month from Bronze (incremental)
# 2. Flag bad rows with data-quality rules (Spark SQL)
# 3. Split into valid and rejected rows
# 4. Remove duplicates, derive new columns, join zone names
# 5. Write Silver + a data-quality log

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 1 - Read only this month from Bronze

# CELL ********************

df_bronze = spark.table("bronze_yellow_trips").where(F.col("load_ym") == ym)
df_bronze.createOrReplaceTempView("v_bronze")
bronze_rows = df_bronze.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 2 - Data-quality rules
# Each row gets a `dq_status`. The first rule that fails wins; otherwise `OK`.

# CELL ********************

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_flagged AS
SELECT *,
  CASE
    WHEN tpep_pickup_datetime IS NULL OR tpep_dropoff_datetime IS NULL        THEN 'missing_datetime'
    WHEN tpep_dropoff_datetime <= tpep_pickup_datetime                        THEN 'dropoff_before_pickup'
    WHEN date_format(tpep_pickup_datetime, 'yyyy-MM') <> '{ym}'               THEN 'pickup_outside_month'
    WHEN trip_distance <= 0 OR trip_distance > 100                            THEN 'bad_distance'
    WHEN fare_amount <= 0 OR total_amount <= 0                                THEN 'bad_amount'
    WHEN pulocationid IS NULL OR dolocationid IS NULL                         THEN 'missing_location'
    WHEN (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60 > 300 THEN 'trip_too_long'
    WHEN trip_distance / ((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600) > 80 THEN 'unrealistic_speed'
    ELSE 'OK'
  END AS dq_status
FROM v_bronze
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# How many rows break each rule?
spark.sql("SELECT dq_status, COUNT(*) AS rows FROM v_flagged GROUP BY dq_status ORDER BY rows DESC").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 3 - Save rejected rows (so nothing silently disappears)

# CELL ********************

df_rejected = spark.sql("SELECT * FROM v_flagged WHERE dq_status <> 'OK'")
rejected_rows = df_rejected.count()

def write_by_month(df, table):
    """Re-runnable write: replaces only the current month."""
    if spark.catalog.tableExists(table):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"load_ym = '{ym}'").saveAsTable(table))
    else:
        df.write.format("delta").partitionBy("load_ym").saveAsTable(table)

write_by_month(df_rejected, "silver_yellow_rejected")
print("Rejected rows:", rejected_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 4 - Build the clean Silver dataset
# - remove duplicates using a hash `trip_id`
# - derive duration, speed, tip %, date parts
# - decode payment type
# - add pickup / dropoff borough and zone names

# CELL ********************

spark.sql("""
CREATE OR REPLACE TEMP VIEW v_clean AS
SELECT
  sha2(concat_ws('|', vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
                 pulocationid, dolocationid, total_amount), 256)            AS trip_id,
  vendorid,
  tpep_pickup_datetime                                                      AS pickup_datetime,
  tpep_dropoff_datetime                                                     AS dropoff_datetime,
  CASE WHEN passenger_count IS NULL OR passenger_count = 0 THEN 1
       ELSE passenger_count END                                             AS passenger_count,
  trip_distance,
  pulocationid, dolocationid,
  payment_type,
  CASE payment_type WHEN 1 THEN 'Credit card' WHEN 2 THEN 'Cash'
                    WHEN 3 THEN 'No charge'   WHEN 4 THEN 'Dispute'
                    ELSE 'Unknown' END                                      AS payment_type_desc,
  fare_amount, tip_amount, tolls_amount, total_amount,
  round((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60, 2) AS trip_duration_min,
  round(trip_distance / ((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600), 2) AS avg_speed_mph,
  round(tip_amount / fare_amount * 100, 2)                                  AS tip_pct,
  to_date(tpep_pickup_datetime)                                             AS pickup_date,
  hour(tpep_pickup_datetime)                                                AS pickup_hour,
  dayofweek(tpep_pickup_datetime)                                           AS day_of_week,
  date_format(tpep_pickup_datetime, 'E')                                    AS day_name,
  load_ym
FROM v_flagged
WHERE dq_status = 'OK'
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_clean = spark.table("v_clean").dropDuplicates(["trip_id"])
df_clean.createOrReplaceTempView("v_clean_dedup")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 5 - Enrich with zone names (join to lookup)

# CELL ********************

df_silver = spark.sql("""
SELECT c.*,
       pz.borough AS pickup_borough,  pz.zone AS pickup_zone,
       dz.borough AS dropoff_borough, dz.zone AS dropoff_zone
FROM v_clean_dedup c
LEFT JOIN bronze_taxi_zones pz ON c.pulocationid = pz.locationid
LEFT JOIN bronze_taxi_zones dz ON c.dolocationid = dz.locationid
""")

silver_rows = df_silver.count()
write_by_month(df_silver, "silver_yellow_trips")
print("Silver rows written:", silver_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Step 6 - Data-quality log (one row per month)

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS silver_dq_log (
  load_ym STRING, bronze_rows BIGINT, rejected_rows BIGINT,
  duplicate_rows BIGINT, silver_rows BIGINT, logged_at TIMESTAMP
) USING DELTA
""")
spark.sql(f"DELETE FROM silver_dq_log WHERE load_ym = '{ym}'")

duplicate_rows = bronze_rows - rejected_rows - silver_rows
spark.sql(f"""
INSERT INTO silver_dq_log
VALUES ('{ym}', {bronze_rows}, {rejected_rows}, {duplicate_rows}, {silver_rows}, current_timestamp())
""")
spark.sql("SELECT * FROM silver_dq_log ORDER BY load_ym").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
