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

pitstops_schema = StructType(fields =[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True)
                                     ])

# COMMAND ----------

pitstops_df = spark.read.option("header", "true") \
.option("multiline", "true") \
.schema(pitstops_schema) \
.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json')

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

pitstops_final_df = pitstops_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(pitstops_final_df)

# COMMAND ----------

pitstops_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/pitstops").saveAsTable("pitstops_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended pitstops_ext;
