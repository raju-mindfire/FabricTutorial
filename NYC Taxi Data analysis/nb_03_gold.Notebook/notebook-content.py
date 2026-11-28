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

# # nb_03_gold : Business-ready aggregates
# **Gold rule:** small, fast tables that answer business questions. Power BI / reports read from here.
# 
# | Gold table | Business question |
# |---|---|
# | `gold_daily_summary` | How do trips and revenue move day by day? |
# | `gold_hourly_demand` | Which hours / weekdays are busiest? |
# | `gold_zone_summary` | Which boroughs and zones earn the most? |
# | `gold_payment_summary` | Card vs cash: who tips more? |
# | `gold_top_routes` | What are the most popular routes? |
# 
# The last cell moves the **watermark** forward, which is what makes the next pipeline run pick the next month.

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

ym = f"{year}-{month}"

def write_gold(df, table):
    """Replace only the current month inside the gold table."""
    if spark.catalog.tableExists(table):
        (df.write.format("delta").mode("overwrite")
           .option("replaceWhere", f"load_ym = '{ym}'").saveAsTable(table))
    else:
        df.write.format("delta").partitionBy("load_ym").saveAsTable(table)
    print("written:", table, "->", df.count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1. Daily summary

# CELL ********************

df = spark.sql(f"""
SELECT load_ym, pickup_date,
       COUNT(*)                          AS total_trips,
       SUM(passenger_count)              AS total_passengers,
       ROUND(SUM(total_amount), 2)       AS total_revenue,
       ROUND(AVG(fare_amount), 2)        AS avg_fare,
       ROUND(AVG(trip_distance), 2)      AS avg_distance_miles,
       ROUND(AVG(trip_duration_min), 2)  AS avg_duration_min,
       ROUND(AVG(tip_pct), 2)            AS avg_tip_pct
FROM silver_yellow_trips
WHERE load_ym = '{ym}'
GROUP BY load_ym, pickup_date
""")
write_gold(df, "gold_daily_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Hourly demand by weekday

# CELL ********************

df = spark.sql(f"""
SELECT load_ym, day_of_week, day_name, pickup_hour,
       COUNT(*)                     AS trips,
       ROUND(AVG(fare_amount), 2)   AS avg_fare,
       ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph
FROM silver_yellow_trips
WHERE load_ym = '{ym}'
GROUP BY load_ym, day_of_week, day_name, pickup_hour
""")
write_gold(df, "gold_hourly_demand")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Pickup zone / borough summary

# CELL ********************

df = spark.sql(f"""
SELECT load_ym, pickup_borough, pickup_zone,
       COUNT(*)                     AS trips,
       ROUND(SUM(total_amount), 2)  AS total_revenue,
       ROUND(AVG(fare_amount), 2)   AS avg_fare,
       ROUND(AVG(tip_pct), 2)       AS avg_tip_pct
FROM silver_yellow_trips
WHERE load_ym = '{ym}'
GROUP BY load_ym, pickup_borough, pickup_zone
""")
write_gold(df, "gold_zone_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Payment type summary

# CELL ********************

df = spark.sql(f"""
SELECT load_ym, payment_type_desc,
       COUNT(*)                    AS trips,
       ROUND(SUM(total_amount), 2) AS total_revenue,
       ROUND(AVG(tip_amount), 2)   AS avg_tip_amount,
       ROUND(AVG(tip_pct), 2)      AS avg_tip_pct
FROM silver_yellow_trips
WHERE load_ym = '{ym}'
GROUP BY load_ym, payment_type_desc
""")
write_gold(df, "gold_payment_summary")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Top 100 routes of the month

# CELL ********************

df = spark.sql(f"""
SELECT load_ym, pickup_zone, dropoff_zone,
       COUNT(*)                   AS trips,
       ROUND(AVG(fare_amount), 2) AS avg_fare,
       ROUND(AVG(trip_distance),2) AS avg_distance_miles
FROM silver_yellow_trips
WHERE load_ym = '{ym}'
GROUP BY load_ym, pickup_zone, dropoff_zone
ORDER BY trips DESC
LIMIT 100
""")
write_gold(df, "gold_top_routes")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Move the watermark forward
# Only reached if every step above succeeded. If any cell fails, the watermark stays unchanged and the next run retries the same month.

# CELL ********************

spark.sql(f"""
INSERT INTO ctl_load_watermark
VALUES ('yellow_taxi', '{ym}', current_timestamp())
""")
spark.sql("SELECT * FROM ctl_load_watermark ORDER BY last_loaded_ym").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
