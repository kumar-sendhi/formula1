# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.processed.races_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.processed.results_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.processed.results_ext join formula1.processed.drivers_ext on (formula1.processed.results_ext.driver_id = formula1.processed.drivers_ext.driver_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.processed.results_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC create  table if not exists formula1.presentation.calculated_race_results_ext 
# MAGIC Using delta
# MAGIC Location 'abfss://presentation@externaltableseyon.dfs.core.windows.net/calculated_race_results'
# MAGIC AS
# MAGIC select races_ext.year,
# MAGIC constructors_ext.constructor_name,
# MAGIC drivers_ext.driver_name,
# MAGIC results_ext.position,
# MAGIC results_ext.points,
# MAGIC 11 - results_ext.position as calculated_points
# MAGIC from formula1.processed.results_ext join formula1.processed.drivers_ext on (formula1.processed.results_ext.driver_id = formula1.processed.drivers_ext.driver_id)
# MAGIC join formula1.processed.constructors_ext on (formula1.processed.results_ext.constructor_id = formula1.processed.constructors_ext.constructor_id)
# MAGIC join formula1.processed.races_ext on (formula1.processed.results_ext.race_id = formula1.processed.races_ext.racesId)
# MAGIC where formula1.processed.results_ext.position <= 10
