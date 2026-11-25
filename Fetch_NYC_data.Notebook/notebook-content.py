# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "33294ffb-1bfb-4a4a-acb3-e9e607695d04",
# META       "default_lakehouse_name": "bronze_lake",
# META       "default_lakehouse_workspace_id": "14524749-a634-4345-bccf-4407b30d1ca9",
# META       "known_lakehouses": [
# META         {
# META           "id": "33294ffb-1bfb-4a4a-acb3-e9e607695d04"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
fileName = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = spark.read.parquet('Files/NYC_Yellow/File_2024-01.parquet')
# src.cache()
spark.catalog.clearCache()
src.unpersist()
display(src.count())
display(src.count())
src.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = spark.read.parquet('Files/NYC_Yellow/File_2024-01.parquet')
src.cache()
display(src.count())
display(src.count())
src.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = spark.read.parquet('Files/NYC_Yellow/File_2024-01.parquet')
src.createTempView("MyTMPVW")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- SELECT * FROM Parquet.`Files/NYC_Yellow/File_2024-01.parquet`
# MAGIC -- DESCRIBE HISTORY mytesttable
# MAGIC 
# MAGIC 
# MAGIC -- UPDATE mytesttable
# MAGIC -- SET trip_distance = 1.75
# MAGIC --   WHERE trip_distance = 1.72
# MAGIC 
# MAGIC DESCRIBE HISTORY mytesttable

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM mytesttable WHERE trip_distance = 1.75
# MAGIC -- RESTORE TABLE mytesttable VERSION AS OF 0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

src = spark.read.parquet('Files/NYC_Yellow/File_2024-01.parquet')
src.cache()
display(src.count())  # 2964624

df = src.where(src.PULocationID == src.DOLocationID)
display(df) #

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bronze_lake.dbo.jointable LIMIT 1000")
df2 = spark.sql("SELECT * FROM bronze_lake.dbo.jointable LIMIT 1000")
df.join(df2, df.VendorID == df2.VendorID, 'Full').select(df.VendorID, df.trip_distance).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("dbo.jointable")
sf = spark.read.table("dbo.jointable")

res = df.join(sf, df.VendorID == sf.VendorID, 'Full')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT f.VendorID, s.VendorID FROM bronze_lake.dbo.jointable f 
# MAGIC INNER JOIN bronze_lake.dbo.jointable s ON s.VendorID = f.VendorID
# MAGIC LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT * FROM nyc_yellow LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("nyc_yellow")
df.cache()

ds = df.limit(10) #.agg(F.sum("trip_distance").alias("Total Sum Distance")).limit(10)
display(ds)
ds.agg(F.sum("trip_distance").alias("Total"), F.("trip_distance").alias("DistinctRow")).show()

ds.groupBy()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
