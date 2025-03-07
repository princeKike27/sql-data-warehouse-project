/*
=====================================================================
Ingestion Script: Ingest Data from .csv files to DB Tables
=====================================================================

Script Purpose:
	This script uses BULK INSERT to load the data from the crm and erp .csv files
	into their corresponding tables on the bronze layer. Run this sript to populate
	the tables with the original data from the sources.

WARNING:
	 Proceed with caution, the script will delete all records from the tables
	 and load the data that is on the .csv files. Ensure you have proper backups 
	 before running the script.

*/


---------------------------------- CRM SOURCE --------------------------------

----------------------------------------
-- Ingest data from cust_info.csv
TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\gomeze82\OneDrive - Medtronic PLC\6. Warehouse - Pro\datasets\source_crm\cust_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	-- lock table when loading
	TABLOCK
);
-- check insert
SELECT * FROM bronze.crm_cust_info;
GO

----------------------------------------
-- Ingest data from prd_info.csv
TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\gomeze82\OneDrive - Medtronic PLC\6. Warehouse - Pro\datasets\source_crm\prd_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	-- lock table when loading
	TABLOCK
);
-- check insert
SELECT * FROM bronze.crm_prd_info;
GO

----------------------------------------
-- Ingest data from sales_details.csv
TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\gomeze82\OneDrive - Medtronic PLC\6. Warehouse - Pro\datasets\source_crm\sales_details.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	-- lock table when loading
	TABLOCK
);
GO
-- check insert
SELECT * FROM bronze.crm_sales_details;
GO


---------------------------------- ERP SOURCE --------------------------------

----------------------------------------
-- Ingest data from cust_az12.csv
TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'C:\Users\gomeze82\OneDrive - Medtronic PLC\6. Warehouse - Pro\datasets\source_erp\cust_az12.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	-- lock table when loading
	TABLOCK
);
GO
-- check insert
SELECT * FROM bronze.erp_cust_az12; 
GO

----------------------------------------
-- Ingest data from loc_a101.csv
TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\Users\gomeze82\OneDrive - Medtronic PLC\6. Warehouse - Pro\datasets\source_erp\loc_a101.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	-- lock table when loading
	TABLOCK
);
GO
--check insert
SELECT * FROM bronze.erp_loc_a101;
GO


----------------------------------------
-- Ingest data from px_cat_g1v2.csv
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\Users\gomeze82\OneDrive - Medtronic PLC\6. Warehouse - Pro\datasets\source_erp\px_cat_g1v2.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	-- lock table when loading
	TABLOCK
);
GO
--check insert
SELECT * FROM bronze.erp_px_cat_g1v2;
GO
