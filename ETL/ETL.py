# Databricks notebook source
# MAGIC %md
# MAGIC #Task 2.2

# COMMAND ----------

# MAGIC %md
# MAGIC ###The task involve the database configuration, reading the properties json file and loading it to the database.

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

# MAGIC %md
# MAGIC #Task 2.3

# COMMAND ----------

# MAGIC %md
# MAGIC ###The task involves selecting 5 properties from the above property data frame, fetching their daily energy usage of the last 14 days using API endpoints. Creating an appropriate table and storing the fetched data in the database table.

# COMMAND ----------


sample_properties_choosen = cleansed_property_df.head(5)


# COMMAND ----------

display(sample_properties_choosen)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Blank data frame creation for corner cases and exception handling.

# COMMAND ----------

from pyspark.sql.types import StringType, StructField, StringType, FloatType, StructType, DateType

def f_get_blank_df()->DataFrame:
    schema = StructType([
        StructField('propertyCode', StringType(), True),
        StructField('locationName', StringType(), True),
        StructField('recordedDate', DateType(), True),
        StructField('EnergyType', StringType(), True),
        StructField('Value', FloatType(), True),
    ])
    return spark.createDataFrame([], schema = schema)


# COMMAND ----------

# MAGIC %md
# MAGIC #####The function fetches the data using a get request. Heat and Electricity energy data has been fetched. The Water and DistrictCooling returned no response for most of the properties, so I have excluded them as those records relevant to those energies are not updated properly on the site, and returned empty responses for most of the properties. But the code is reusable we can easily integrate those energies too.

# COMMAND ----------

# MAGIC %md
# MAGIC ######Also in the below function, in case of successful output, we have cleansed data such as, we do not need timestamp as daily data has been fetched, so it is converted to the date field. Also, some cleansing tasks have been handled directy.

# COMMAND ----------

from pyspark.sql.functions import lit, round, to_date
import requests
from datetime import date, timedelta


def f_fetch_energy_data(propertyCode: str, energy: str)->DataFrame:
    current_date = date.today()
    fortnight_ago_date = current_date - timedelta(days=14)
    try:
        end_points = f"https://helsinki-openapi.nuuka.cloud/api/v1.0/EnergyData/Daily/ListByProperty?Record=PropertyCode&SearchString={propertyCode}&ReportingGroup={energy}&StartTime={fortnight_ago_date}&EndTime={current_date}"
        response = requests.get(end_points)

        if response.status_code == 200:
            data = response.json()

            output_df = spark.read.json(sc.parallelize([data]))\
                .withColumnRenamed('value', 'Value')\
                .withColumn('Value', round(col('Value'), 2))\
                .withColumn('propertyCode', lit(propertyCode))\
                .withColumn('timestamp', to_date(col('timestamp')))\
                .withColumnRenamed('timestamp', 'recordedDate')\
                .withColumnRenamed('reportingGroup', 'EnergyType')\
                .drop('reportingGroup', 'unit')\
                .select(['propertyCode', 'locationName', 'recordedDate', 'EnergyType', 'Value'])
        else:
            print('No result found')
            output_df = f_get_blank_df()

    except Exception as e:
        print(f"Something went wrong: {str(e)}")
        output_df = f_get_blank_df()
    
    return output_df     

# COMMAND ----------

# MAGIC %md
# MAGIC #####Fetching the data from API and appending the data to the data frame.

# COMMAND ----------

formatted_energy_df = f_get_blank_df()

for item in sample_properties_choosen:
    property_energy_electricity_df = f_fetch_energy_data(item[1], "Electricity")
    property_energy_heat_df = f_fetch_energy_data(item[1], "Heat")
    formatted_energy_df = formatted_energy_df.union(property_energy_electricity_df).union(property_energy_heat_df)

# COMMAND ----------

display(formatted_energy_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Since we have been fetching data incrementally for the last 14 days. Only the new records must be inserted into the database.

# COMMAND ----------

energy_table_df = f_read_from_table(property_energy_table)
extracted_data_frame = formatted_energy_df
non_existing_data_item_df = extracted_data_frame.subtract(energy_table_df)

#storage code here
f_write_to_a_table(non_existing_data_item_df, property_energy_table, 'append')

# COMMAND ----------


