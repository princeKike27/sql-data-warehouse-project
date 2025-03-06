/*
============================================================
Create Database and Schemas
============================================================

Script Purpose:
	This script creates a new database named 'DataWarehouse' after checking
	if it already exists. If the database exists, it is dropped and recreated.
	Additionally, the script sets up three schemas within the database: 
	'bronze', 'silver' and 'gold'.


WARNING:
	Running this script will drop the entire 'DataWarehouse' database after
	checking if it already exists. All data in the database will be permanently
	deleted. Proceed with caution and ensure you have proper backups before 
	running the script.

*/

-- use master DB
USE master;
GO

-- drop and recreate the 'DataWarehouse' DB
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;
GO

-- create db 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

-- switch to DataWarehouse
USE DataWarehouse;
GO

----------------------- Creating Schemas ---------------

-- schemas are inside DB >> Security >> Schemas

-- bronze
CREATE SCHEMA bronze;
GO 
-- silver
CREATE SCHEMA silver;
GO
-- gold
CREATE SCHEMA gold;
GO
