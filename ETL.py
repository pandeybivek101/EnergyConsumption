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

# MAGIC %md
# MAGIC #####The property code field contains comma-separated and ja-separated values that should be cleansed first. Also the null and blank on property code should be removed.

# COMMAND ----------

from pyspark.sql.functions import col, explode, split

cleansed_property_df = raw_properties_df.where(col('propertyCode').isNotNull())\
        .where(col('propertyCode') != '')\
        .withColumn("propertyCode", explode(split(col("propertyCode"), ",")))\
        .withColumn("propertyCode", explode(split(col("propertyCode"), " ja ")))

# COMMAND ----------

display(cleansed_property_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Check for any data duplication and if the property code can be set to a unique identification code on the DB.

# COMMAND ----------

from pyspark.sql.functions import count
property_code_duplication_check_df = cleansed_property_df.groupBy('propertyCode').agg(
    count('*').alias('counts')
)
display(property_code_duplication_check_df)

# COMMAND ----------

property_all_column_duplication_check_df = cleansed_property_df.groupBy(['propertyCode', 'locationName', 'propertyName']).agg(
    count('*').alias('counts')
)
display(property_all_column_duplication_check_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Since the above testing verified property code can be repeated.  Also, there is no duplication of data.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configuring the database.

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

database_host = dbutils.secrets.get('secrets', 'DBHost')
database_port = dbutils.secrets.get('secrets', 'DBPort')
database_name = dbutils.secrets.get('secrets', 'DBName')
property_table = "dbo.Property"
property_energy_table = "dbo.PropertyEnergy"
user = dbutils.secrets.get('secrets', 'user')
password = dbutils.secrets.get('secrets', 'password')


url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC #####Functions to read from and write to the database.

# COMMAND ----------

from pyspark.sql import DataFrame

def f_get_options(table_name: str)->dict:
    return {
        "driver": driver,
        "url": url,
        "dbtable": table_name,
        "user": user,
        "password": password
    }


def f_read_from_table(table_name: str)->DataFrame:
    options = f_get_options(table_name)
    return spark.read \
        .format("jdbc") \
        .options(**options) \
        .load()


def f_write_to_a_table(dataFrame: DataFrame, table_name: str, mode: str)->None:
    options = f_get_options(table_name)
    try:
        dataFrame.write \
            .format("jdbc") \
            .options(**options) \
            .mode(mode) \
            .save()
        print("Successfully inserted")
    except Exception as e:
        print(f"Something went wrong: {str(e)}")
    return

# COMMAND ----------

# MAGIC %md
# MAGIC #####Writing dataframe to database.

# COMMAND ----------

f_write_to_a_table(cleansed_property_df, property_table, 'overwrite')

# COMMAND ----------

# property_table_df = f_read_from_table(property_table)
# display(property_table_df)

# COMMAND ----------
