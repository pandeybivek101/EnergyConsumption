# EnergyConsumption
The project is intended to develop a data engineering solution (ETL pipeline).

## Task 2.2
The task involves retrieving the data item from the JSON file, cleansing it, and storing the database. Azure key vault has been utilized to store the DB configuration credentials. This operation was done in separate branch dedicated to the task and later merged to the main branch after completion.

## Task 2.3
The task involves fetching daily energy usage using the get request, parameters such as propertyCode, startDate, endDate have been used to fetch relevant data. The corner cases and failure on the get requests have been handled. The successfully fetched data are appended together and stored in the PropertyEnergy table after cleansing. The task was done in a separate branch and later merged after completion.

## Task 2.4
The tables are filtered and then joined. An alternative to the join approach is also suggested.

