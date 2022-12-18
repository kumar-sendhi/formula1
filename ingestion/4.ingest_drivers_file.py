# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the csv file using the spark dataframe reader

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

name_schema = StructType(fields =[StructField("forename",StringType(),False),
                                     StructField("surname",StringType(),True)
                                     ])

# COMMAND ----------

drivers_schema = StructType(fields =[StructField("driverId",IntegerType(),False),
                                     StructField("driverRef",StringType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("code",StringType(),True),
                                     StructField("name",name_schema),
                                     StructField("dob",DateType(),True),
                                     StructField("nationality",StringType(),True),
                                     StructField("url",StringType(),True)
                                     ])

# COMMAND ----------

drivers_df = spark.read.option("header", "true") \
.schema(drivers_schema) \
.json(f'{raw_folder_path}/{v_file_date}/drivers.json')

# COMMAND ----------

drivers_df.show()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_selected_df = drivers_df.drop('url')

# COMMAND ----------

driversconstructors_renamed_df = drivers_selected_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))) \
.withColumnRenamed("name","driver_name")

# COMMAND ----------

drivers_final_df = driversconstructors_renamed_df.withColumn("data_source",lit(v_data_source)) \
.withColumn("env",lit("Production")) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").option("path","abfss://processed@externaltableseyon.dfs.core.windows.net/drivers").saveAsTable("drivers_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended drivers_ext;
