# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date =dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_data_source","testing")
v_data_source =dbutils.widgets.get("p_data_source")

# COMMAND ----------

# Set the current catalog.
spark.sql("USE CATALOG formula1")

# COMMAND ----------

# Set the current schema.
spark.sql("USE processed")

# COMMAND ----------

drivers_df = spark.table("drivers_ext")\
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df = spark.table("constructors_ext") \
.withColumnRenamed("nationality","constructor_nationality") \
.withColumnRenamed("constructor_name","team")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

circuits_df= spark.table("circuits_ext") \
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df= spark.table("races_ext") \
.withColumnRenamed("races_name","race_name") \
.withColumnRenamed("race_timestamp","race_date") \
.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("racesId","races_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("date","race_date")

# COMMAND ----------

display(races_df)

# COMMAND ----------

results_df= spark.table("results_ext") \
.filter(f"file_date='{v_file_date}'") \
.withColumnRenamed("time","race_time") \
.withColumnRenamed("race_id","result_race_id")

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
.select(races_df.races_id,races_df.race_year, races_df.race_name, races_df.race_date,circuits_df.circuit_location)

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id ==race_circuits_df.races_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp,lit

# COMMAND ----------

final_df =race_results_df.select("races_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time","points","position").withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2011 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# Set the current schema.
spark.sql("USE presentation")

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").option("path",f'{presentation_folder_path}/race_results').saveAsTable("race_results_ext")

# COMMAND ----------

presentation_folder_path

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended race_results_ext;

# COMMAND ----------

race_results_df = spark.table("race_results_ext")

# COMMAND ----------

display(race_results_df)
