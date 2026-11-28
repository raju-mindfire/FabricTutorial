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

# # nb_04_analysis : Insights from the Gold layer
# Run this after at least 2-3 months are loaded. In each result, use the **chart** toggle under the table (Fabric built-in charts).

# MARKDOWN ********************

# ### 1. Monthly trend - trips, revenue and revenue per trip

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT load_ym,
# MAGIC        SUM(total_trips)                                   AS trips,
# MAGIC        ROUND(SUM(total_revenue))                          AS revenue,
# MAGIC        ROUND(SUM(total_revenue) / SUM(total_trips), 2)    AS revenue_per_trip
# MAGIC FROM gold_daily_summary
# MAGIC GROUP BY load_ym
# MAGIC ORDER BY load_ym

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2. Which hours are busiest? (bar chart: pickup_hour vs trips)

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT pickup_hour, SUM(trips) AS trips, ROUND(AVG(avg_fare), 2) AS avg_fare
# MAGIC FROM gold_hourly_demand
# MAGIC GROUP BY pickup_hour
# MAGIC ORDER BY pickup_hour

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3. Weekday vs hour demand (use pivot/heat view or compare lines)

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT day_of_week, day_name, pickup_hour, SUM(trips) AS trips
# MAGIC FROM gold_hourly_demand
# MAGIC GROUP BY day_of_week, day_name, pickup_hour
# MAGIC ORDER BY day_of_week, pickup_hour

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4. Borough performance

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT pickup_borough,
# MAGIC        SUM(trips)                         AS trips,
# MAGIC        ROUND(SUM(total_revenue))          AS revenue,
# MAGIC        ROUND(SUM(total_revenue) / SUM(trips), 2) AS revenue_per_trip
# MAGIC FROM gold_zone_summary
# MAGIC GROUP BY pickup_borough
# MAGIC ORDER BY revenue DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5. Top 10 pickup zones

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT pickup_zone, pickup_borough, SUM(trips) AS trips, ROUND(SUM(total_revenue)) AS revenue
# MAGIC FROM gold_zone_summary
# MAGIC GROUP BY pickup_zone, pickup_borough
# MAGIC ORDER BY trips DESC
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 6. Do card customers tip? (cash tips are not recorded in the data)

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT payment_type_desc,
# MAGIC        SUM(trips)                         AS trips,
# MAGIC        ROUND(AVG(avg_tip_pct), 2)         AS avg_tip_pct
# MAGIC FROM gold_payment_summary
# MAGIC GROUP BY payment_type_desc
# MAGIC ORDER BY trips DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 7. Most popular routes

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT pickup_zone, dropoff_zone, SUM(trips) AS trips, ROUND(AVG(avg_fare), 2) AS avg_fare
# MAGIC FROM gold_top_routes
# MAGIC GROUP BY pickup_zone, dropoff_zone
# MAGIC ORDER BY trips DESC
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 8. Data quality monitor - how much data did we reject?

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT load_ym, bronze_rows, rejected_rows, duplicate_rows, silver_rows,
# MAGIC        ROUND(rejected_rows * 100.0 / bronze_rows, 2) AS rejected_pct
# MAGIC FROM silver_dq_log
# MAGIC ORDER BY load_ym

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
