# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### SILVER LAYER SCRIPT
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access using app

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.awstoragedatalakekha.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.awstoragedatalakekha.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.awstoragedatalakekha.dfs.core.windows.net", "cd542115-c1cb-4afc-9c20-e000abb83a49")
spark.conf.set("fs.azure.account.oauth2.client.secret.awstoragedatalakekha.dfs.core.windows.net", "4xd8Q~CsByqQEqP5lPR3Ozdpi6pQXQvBa3tzDduW")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.awstoragedatalakekha.dfs.core.windows.net", "https://login.microsoftonline.com/6b2a8d53-1a1e-4b5f-82f4-fd99402eb10a/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data

# COMMAND ----------

df_cal = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Calendar")

# COMMAND ----------

df_cust = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_prod = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_prodcat = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/Product_Categories")

# COMMAND ----------

df_prodsubcat = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_returns = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Returns.csv")

# COMMAND ----------

df_sales_15 = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Sales_2015")

# COMMAND ----------

df_sales_16 = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Sales_2016")

# COMMAND ----------

df_sales = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Sales*")

# COMMAND ----------

df_ter = spark.read.format('csv').option("header",True).option("inferSchema",True).load("abfss://bronze@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Calender

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal = df_cal.withColumn('Month',month(col('Date')))\
                .withColumn('Year',year(col('Date')))
df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Calendar")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust.withColumn('fullname',concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

# COMMAND ----------

df_cust = df_cust.withColumn('fullname',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_cust.display()

# COMMAND ----------

df_cust.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Customers")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sub Categories 

# COMMAND ----------

df_prodsubcat.display()

# COMMAND ----------

df_prodsubcat   .write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/Product_Subcategories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Products

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod = df_prod.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
                  .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Products")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Returns 

# COMMAND ----------

df_returns.display()

# COMMAND ----------

df_returns.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Returns.csv")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Territories

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Territories")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales

# COMMAND ----------

df_sales.display()


# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales = df_sales.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))

# COMMAND ----------

df_sales = df_sales.withColumn('multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet')\
            .mode('append')\
            .option("path","abfss://silver@awstoragedatalakekha.dfs.core.windows.net/AdventureWorks_Sales")\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_Order')).display()

# COMMAND ----------

df_prodcat.display()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

