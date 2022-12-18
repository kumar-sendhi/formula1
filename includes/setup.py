# Databricks notebook source
spark.sql("create catalog if not exists formula1")

# COMMAND ----------

spark.sql("USE catalog formula1")

# COMMAND ----------

display(spark.sql("show catalogs"))

# COMMAND ----------

# Grant create and use catalog permissions for the catalog to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT CREATE, USE CATALOG
  ON CATALOG formula1
  TO `formula1_developers`""")

# COMMAND ----------

# Show grants on the quickstart catalog.
display(spark.sql("SHOW GRANT ON CATALOG formula1"))

# COMMAND ----------

# Create a schema in the catalog that was set earlier.
spark.sql("""
  CREATE SCHEMA IF NOT EXISTS processed
  COMMENT 'A new Unity Catalog schema called processed'""")

# COMMAND ----------

# Show schemas in the catalog that was set earlier.
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# Describe the schema.
display(spark.sql("DESCRIBE SCHEMA EXTENDED processed"))

# COMMAND ----------

# Grant create table, create view, and use schema permissions for the schema to all users on the account.
# This also works for other account-level groups and individual users.
spark.sql("""
  GRANT CREATE TABLE, CREATE VIEW, USE SCHEMA
  ON SCHEMA processed
  TO `formula1_developers`""")

# COMMAND ----------

spark.sql("""
  CREATE SCHEMA IF NOT EXISTS presentation
  COMMENT 'A new Unity Catalog schema called presentation'""")

# COMMAND ----------

# Describe the schema.
display(spark.sql("DESCRIBE SCHEMA EXTENDED presentation"))
