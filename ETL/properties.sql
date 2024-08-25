--Database creation if not already exists.
IF db_ID('EnergyConsumption') IS NULL
	BEGIN
		CREATE DATABASE EnergyConsumption;
	END
GO



--Table creation for the property table
IF OBJECT_ID('EnergyConsumption.dbo.Property', 'U') IS NULL
	BEGIN
		CREATE TABLE EnergyConsumption.dbo.Property (
			propertyCode VARCHAR(100),
			locationName VARCHAR(100), 
			propertyName VARCHAR(100)
		)
	END
GO


--Table creation for the energy consumption table
IF OBJECT_ID('EnergyConsumption.dbo.PropertyEnergy', 'U') IS NULL
	BEGIN
		CREATE TABLE  EnergyConsumption.dbo.PropertyEnergy (
			propertyCode VARCHAR(100),
			recordedDate DATE,
			locationName VARCHAR(100),
			EnergyType VARCHAR(100),
			Value FLOAT
		)
	END
GO
