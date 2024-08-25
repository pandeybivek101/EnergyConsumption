# Databricks notebook source
# MAGIC %md
# MAGIC #Task2.1

# COMMAND ----------

# MAGIC %md
# MAGIC ###The task involve the database configuration, reading the properties json file and loading it the the database.

# COMMAND ----------

#Reading the properties.json file

filePath = dbutils.secrets.get('secrets', 'filePath')
raw_properties_df = spark.read.json(filePath, multiLine=True)

# COMMAND ----------

display(raw_properties_df)

# COMMAND ----------