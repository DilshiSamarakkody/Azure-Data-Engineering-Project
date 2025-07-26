# Databricks notebook source
# MAGIC %md
# MAGIC # Silver layer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access data using app

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adventureworksstorageacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adventureworksstorageacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adventureworksstorageacc.dfs.core.windows.net", "54dd8089-baab-45d2-8bf3-0feb241e955d")
spark.conf.set("fs.azure.account.oauth2.client.secret.adventureworksstorageacc.dfs.core.windows.net", "7cW8Q~MLmoHQwhl8yQFQkjiRECDOt8ekyX6vdaHq")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adventureworksstorageacc.dfs.core.windows.net", "https://login.microsoftonline.com/faa7476e-2afe-4fc7-91b1-f2bce069356b/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import the libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

df_cal = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Calendar")
display(df_cal)

# COMMAND ----------

df_cus = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Customers")
display(df_cus)

# COMMAND ----------

df_cat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Product_Categories")
display(df_cat)

# COMMAND ----------

df_subcat = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Product_Subcategories")
display(df_subcat)

# COMMAND ----------

df_prd = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Products")
display(df_prd)

# COMMAND ----------

df_ret = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Returns")
display(df_ret)

# COMMAND ----------

df_sal_15 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Sales_2015")
display(df_sal_15)

# COMMAND ----------

df_sal_16 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Sales_2016")
display(df_sal_16)

# COMMAND ----------

df_sal_17 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Sales_2017")
display(df_sal_17)

# COMMAND ----------

df_ter = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://bronze@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Territories")
display(df_ter)

# COMMAND ----------

duplicates = df_sal_17.groupBy("OrderNumber").count().filter("count > 1").count()
display(duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transform data

# COMMAND ----------

df_cal = df_cal.withColumn("Month",month(df_cal.Date)).withColumn("Year",year(df_cal.Date))
df_cal = df_cal.withColumn("Month",month(col("Date")))\
               .withColumn("Year",year(col("Date")))    
df_cal.show()

# COMMAND ----------

# load calender data to silver layer
df_cal.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Calender")


# COMMAND ----------

df_cus = df_cus.withColumn("FullName",concat_ws(" ",df_cus.Prefix,df_cus.FirstName,df_cus.LastName))
df_cus = df_cus.withColumn("MaritalStatus", when(col('MaritalStatus') == 'M', 'Married')\
                                            .when(col('MaritalStatus') == 'S', 'Single')\
                                            .otherwise(lit('NA')))
df_cus = df_cus.withColumn("Gender", when(col('Gender') == 'M', 'Male')\
                                    .when(col('Gender') == 'F', 'Female')\
                                    .otherwise(col('Gender')))
df_cus = df_cus.withColumn("HomeOwner", when(col('HomeOwner') == 'Y', 'Yes')\
                                       .when(col('HomeOwner') == 'N', 'No')\
                                       .otherwise(lit('NA')))
                                           
display(df_cus)

# COMMAND ----------

# from pyspark.sql.functions import col, trim

# df_cus.filter(col('Prefix').isNull() | (trim(col('Prefix')) == '')).display()

# df_cus.filter(col('Prefix').isNull()).count()-130
# df_cus.filter(col('prefix').isNull()).display()
# df_cus.select(col('homeowner')).distinct().display()   
# df_cus.filter(col('FirstName').isNull()).display()
##df_cus.filter(col('Fullname').isNull()).display()

# COMMAND ----------

## load customer data to silver layer

df_cus.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Customers")

display(df_cus)

# COMMAND ----------

# load product subcategory data to silver layer

df_subcat.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Product_Subcategories")

display(df_subcat)

# COMMAND ----------

df_prd = df_prd.withColumn('productSku',split(col('productSKU'),'-')[0])\
               .withColumn('productName',split(col('productName'),' ')[0])\
               .withColumn('productStyle', when(upper(col('productStyle')) == 'U','Unisex')
                                            .when(upper(col('productStyle')) == 'W','Women')
                                            .when(upper(col('productStyle')) == 'M','Men')
                                            .otherwise(col('ProductStyle')))
df_prd = df_prd.drop('ProductSize')
  
df_prd.display()             

# COMMAND ----------

# load product data to silver layer
df_prd.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Products")

display(df_prd)


# COMMAND ----------

# load return data to silver layer
df_ret.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Returns")

display(df_ret)

# COMMAND ----------

## load territory data to silver layer
df_ter.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Territories")

display(df_ter)

# COMMAND ----------

# load category data to silver layer
df_cat.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_Product_Categories")

display(df_cat)

# COMMAND ----------

df_sales = df_sal_15.union(df_sal_16).union(df_sal_17)
display(df_sales)

# COMMAND ----------

df_sales = df_sales.withColumn("StockDate", to_date(col("StockDate")))
df_sales = df_sales.withColumn("OrderNumber", regexp_replace(col("OrderNumber"),'S','T'))
df_sales = df_sales.withColumn("Multiply", col("OrderQuantity")*col("OrderLineItem"))
display(df_sales)

# COMMAND ----------

# load sales data to silver layer
df_sales.write.format("parquet")\
            .mode("overwrite")\
            .save("abfss://silver@adventureworksstorageacc.dfs.core.windows.net/AdventureWorks_sales")

display(df_sales)

# COMMAND ----------

df_sales.groupBy("orderDate").agg(count("OrderNumber").alias("Total orders")).display()  

# COMMAND ----------

df_ter.display()