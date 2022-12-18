# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType
from pyspark.sql.functions import col, current_timestamp,lit,to_timestamp,concat

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

constructors_schema = 'constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df = spark.read.option("header", "true") \
.schema(constructors_schema) \
.json(f'{raw_folder_path}/{v_file_date}/constructors.json')

# COMMAND ----------

constructors_df.show()

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_selected_df = constructors_df.drop('url')

# COMMAND ----------

display(constructors_selected_df)

# COMMAND ----------

constructors_renamed_df = constructors_selected_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumnRenamed("name","constructor_name") 

# COMMAND ----------

constructors_final_df = constructors_renamed_df.withColumn("data_source",lit(v_data_source)) \
.withColumn("env",lit("Production")) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/constructors").saveAsTable("constructors_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended constructors_ext;
