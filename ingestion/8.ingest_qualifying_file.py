# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType,FloatType
from pyspark.sql.functions import col, current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

dbutils.widgets.text("p_data_source","testing")
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

qualifying_schema = StructType(fields =[StructField("qualifyId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)
                                     ])

# COMMAND ----------

qualifying_df = spark.read.option("header", "true") \
.option("multiline", "true") \
.schema(qualifying_schema) \
.json(f'{raw_folder_path}/{v_file_date}/qualifying')

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

qualifying_final_df = qualifying_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/qualifying").saveAsTable("qualifying_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended qualifying_ext;
