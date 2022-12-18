# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType,FloatType
from pyspark.sql.functions import col, current_timestamp,lit,to_timestamp,concat

# COMMAND ----------

dbutils.widgets.text("p_data_source","Production")
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

results_schema = StructType(fields =[StructField("resultId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("grid",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("positionText",StringType(),True),
                                     StructField("positionOrder",IntegerType(),True),
                                     StructField("points",FloatType(),True),
                                     StructField("laps",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True),
                                     StructField("fastestLap",IntegerType(),True),
                                     StructField("rank",IntegerType(),True),
                                     StructField("fasterLapTime",StringType(),True),
                                     StructField("fasterLapSpeed",FloatType(),True),
                                     StructField("statusId",StringType(),True)
                                     ])

# COMMAND ----------

results_df = spark.read.option("header", "true") \
.schema(results_schema) \
.json(f'{raw_folder_path}/{v_file_date}/results.json')

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_renamed_df = results_df.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("ingestion_date",current_timestamp())

# COMMAND ----------

results_final_df = results_renamed_df.withColumn("env",lit("Production")).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date)).drop('statusId')

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

results_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/results").saveAsTable("results_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended results_ext;
