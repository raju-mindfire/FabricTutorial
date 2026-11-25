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
# META         },
# META         {
# META           "id": "75b492d1-ab37-420f-8221-c5bd4657a46c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import Window as w
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bronze_lake.dbo.aw_person_person LIMIT 3")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("aw_sales_customer") # Customer table

display(df.where(df.CustomerID == 29825))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# aw_sales_customer Customer table
# aw_sales_salesorderheader order table
#

df = spark.sql("SELECT * FROM bronze_lake.dbo.aw_production_product LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bronze_lake.dbo.awdw_dimproduct LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM bronze_lake.dbo.aw_sales_salesorderheader LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product = spark.read.table("aw_production_product")
salesOrder = spark.read.table("aw_sales_salesorderdetail")
sales_salesorderheader = spark.read.table("aw_sales_salesorderheader")
customer = spark.read.table("aw_sales_customer")
territory = spark.read.table("aw_sales_salesterritory")
person = spark.read.table("aw_person_person")
product_salesorder = product.join(salesOrder, on="ProductID", how="inner")
.join(sales_salesorderheader, on="SalesOrderID", how="inner")
.join(customer, on="CustomerID", how="inner")
.join(territory, on="TerritoryID", how="inner")
.join(person, person.BusinessEntityID == customer.PersonID, how="inner")
.select(F.col("SubTotal").alias("OrderAmount"), 
F.col("OrderDate"), F.col("SalesOrderID").alias("OrderId"), F.col("aw_sales_salesterritory.Name").alias("Region"), F.col("aw_person_person.Title"), F.col("aw_person_person.FirstName"), F.col("aw_person_person.MiddleName"), F.col("aw_person_person.LastName").alias("Customer Name"), F.col("aw_person_person.BusinessEntityID").alias("Customer ID"))


# display(customer)
display(product_salesorder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

person = spark.read.table("aw_person_person")
customers_without_orders = customer.join(
    sales_salesorderheader,
    on="CustomerID",
    how="left_anti"
)

display(customers_without_orders)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

product = spark.read.table("aw_production_product")
salesOrder = spark.read.table("aw_sales_salesorderdetail")
sales_salesorderheader = spark.read.table("aw_sales_salesorderheader")
customer = spark.read.table("aw_sales_customer")
person = spark.read.table("aw_person_person")
product_salesorder = product.join(salesOrder, on="ProductID", how="inner").join(sales_salesorderheader, on="SalesOrderID", how="inner").join(customer, on="CustomerID", how="inner").join(person, person.BusinessEntityID == customer.PersonID, how="inner").select(F.col("SubTotal").alias("OrderAmount"), F.col("OrderDate"), F.col("SalesOrderID").alias("OrderId"), F.col("aw_person_person.Title"), F.col("aw_person_person.FirstName"), F.col("aw_person_person.MiddleName"), F.col("aw_person_person.LastName").alias("Customer Name"), F.col("aw_person_person.BusinessEntityID").alias("Customer ID"))


# display(customer)
display(product_salesorder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_salesorderheader = spark.read.table("aw_sales_salesorderheader")

# display(customer)
df = sales_salesorderheader.groupBy(sales_salesorderheader.CustomerID).agg(
        F.count("SalesOrderID").alias("TotalOrders")
    ).orderBy(F.col("TotalOrders").desc())
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# product = spark.read.table("aw_production_product")
# salesOrder = spark.read.table("aw_sales_salesorderdetail")

sales_salesorderheader = spark.read.table("aw_sales_salesorderheader") # sales details
customer = spark.read.table("aw_sales_customer") # Customer details
territory = spark.read.table("aw_sales_salesterritory") # customer territory

jn = sales_salesorderheader.join("customer", on="CustomerID", how="inner").join("territory", on="TerritoryID", how="inner")

p = w.partitionBy("TerritoryID").orderBy(F.col("SubTotal").desc(), )

display(sales_salesorderheader)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_salesorderheader = spark.read.table("aw_sales_salesorderheader")
customer = spark.read.table("aw_sales_customer")
territory = spark.read.table("aw_sales_salesterritory")
person = spark.read.table("aw_person_person")
store = spark.read.table("aw_sales_store") 

jn = (
    sales_salesorderheader.select("SalesOrderID", "CustomerID", "SubTotal")
    .join(customer.select("CustomerID", "PersonID", "StoreID", "TerritoryID"), on="CustomerID", how="inner")
    .join(territory.select("TerritoryID", F.col("Name").alias("Region")), on="TerritoryID", how="inner")
)

customer_orders = (
    jn.groupBy("Region", "CustomerID", "PersonID", "StoreID")
      .agg(
          F.count("SalesOrderID").alias("OrderCount"),
          F.sum("SubTotal").cast("decimal(19,2)").alias("TotalSales"),
      )
)

p = w.partitionBy("Region").orderBy(F.col("OrderCount").desc(), F.col("TotalSales").desc(), "CustomerID")

res = (
    customer_orders.withColumn("Rank", F.row_number().over(p))
                   .filter(F.col("Rank") <= 3).join(person.select(F.col("BusinessEntityID").alias("PersonID"), "FirstName", "LastName"), "PersonID", "left")
        .join(store.select(F.col("BusinessEntityID").alias("StoreID"), F.col("Name").alias("StoreName")), "StoreID", "left")
        .withColumn("CustomerName",
                    F.when(F.col("FirstName").isNotNull(), F.concat_ws(" ", "FirstName", "LastName"))
                     .otherwise(F.col("StoreName")))
        .select("Region", "Rank", "CustomerID", "CustomerName", "OrderCount", "TotalSales")
        .orderBy("Region", "Rank")
)

display(res)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_salesorderheader = spark.read.table("aw_sales_salesorderheader")
display( 
    sales_salesorderheader.agg(
        F.sum("SubTotal").alias("SubTotal"), 
        F.min("SubTotal").alias("Min"), 
        F.max("SubTotal").alias("Max"), F.avg("SubTotal").alias("Avg"), F.count("SalesOrderId").alias("Total Orders") )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table("aw_sales_salesorderheader").where(F.col("aw_sales_salesorderheader.CustomerId") == 11033))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table("aw_sales_salesorderheader").groupBy("CustomerId").sum("SubTotal"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
