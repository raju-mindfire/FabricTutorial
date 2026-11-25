# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "75b492d1-ab37-420f-8221-c5bd4657a46c",
# META       "default_lakehouse_name": "silver_lake",
# META       "default_lakehouse_workspace_id": "14524749-a634-4345-bccf-4407b30d1ca9",
# META       "known_lakehouses": [
# META         {
# META           "id": "75b492d1-ab37-420f-8221-c5bd4657a46c"
# META         },
# META         {
# META           "id": "33294ffb-1bfb-4a4a-acb3-e9e607695d04"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

fileName = ''
tableName = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read downloaded data file
tbl = spark.table("nyc_yellow")
tbl.cache()
src = spark.read.parquet(fileName)
src.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean data
df = src.where(src.total_amount > 0)
df = df.where(df.trip_distance > 0)
df = df.where(src.PULocationID != src.DOLocationID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Merge Data


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
