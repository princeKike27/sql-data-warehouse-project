/*
=====================================================================
DDL Script: Create Bronze Tables
=====================================================================

Script Purpose:
	This script creates tables in the 'bronze' schema, dropping existing tables
	if the already exist. Run this script to re-define the DDL Structure of 
	'bronze' tables 

WARNING:
	 Proceed with caution and ensure you have proper backups before running 
	 the script.

*/


-- DDL >> Data Definition Language that defines the structure of the DB Tables

---------------------------------- CRM SOURCE --------------------------------

-- Drop table if exists >> 'U': User Defined Table
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO
-- crm_cust_info Table
CREATE TABLE bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE
);
GO

-- Drop table if exists
IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info
GO
-- crm_prd_info Table
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE
);
GO

-- Drop table if exists
IF OBJECT_ID('bronze.cmr_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details
GO
-- crm_sales_details Table
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

---------------------------------- ERP SOURCE --------------------------------

-- Drop table if exists
IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
GO
-- erp_cust_az12 Table
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO

-- Drop table if exists
IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
GO
-- erp_loc_a101 Table
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO

-- Drop table if exists
IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
GO
-- erp_px_cat_g1v2 Table
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO
