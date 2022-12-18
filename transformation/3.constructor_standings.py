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
spark.sql("USE presentation")

# COMMAND ----------

race_results_df= spark.table("race_results_ext")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, when,count,col
constructor_standings_df = race_results_df \
.groupBy("race_year","team") \
.agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))

# COMMAND ----------

display(constructor_standings_df.filter("race_year=2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank


constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").option("path",f'{presentation_folder_path}/constructor_standings').saveAsTable("constructor_standings_ext")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE extended constructor_standings_ext;
