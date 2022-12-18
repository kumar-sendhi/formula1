# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source =dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG formula1")

# COMMAND ----------

# Set the current schema.
spark.sql("USE processed")

# COMMAND ----------

# Show the current database (also called a schema).
spark.catalog.currentDatabase()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

circuits_schema = StructType(fields =[StructField("circuitId",IntegerType(),False),
                                     StructField("circuitRef",StringType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("location",StringType(),True),
                                     StructField("country",StringType(),True),
                                     StructField("lat",DoubleType(),True),
                                     StructField("lng",DoubleType(),True),
                                     StructField("alt",IntegerType(),True),
                                     StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

circuits_df = spark.read.option("header", "true") \
.schema(circuits_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_selected_df = circuits_df.select('circuitId','circuitRef','name','location','country','lat','lng','alt')

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("name","circuits_name") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude") 

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("env",lit("Production")) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/circuits").saveAsTable("circuits_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended circuits_um;

# COMMAND ----------

spark.sql("drop table circuits_um")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended circuits;

# COMMAND ----------

spark.sql("drop table if exists circuits")

# COMMAND ----------



# COMMAND ----------


