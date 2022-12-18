# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,TimestampType,DateType
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

races_schema = StructType(fields =[StructField("racesId",IntegerType(),False),
                                     StructField("year",IntegerType(),True),
                                     StructField("round",IntegerType(),True),
                                     StructField("circuitId",IntegerType(),True),
                                     StructField("name",StringType(),True),
                                     StructField("date",DateType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

races_df = spark.read.option("header", "true") \
.schema(races_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

races_df.show()

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_selected_df = races_df.select('racesId','year','round','circuitId','name','date','time')

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("racesId","races_id") \
.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("name","races_name") \
.withColumnRenamed("year","race_year") \
.withColumn("race_timestamp",to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_final_df = races_selected_df.withColumn("env",lit("Production")) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/races").saveAsTable("races_ext")
