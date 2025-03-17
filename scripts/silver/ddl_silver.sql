/*
=====================================================================
DDL Script: Create Silver Tables
=====================================================================

Script Purpose:
	This script creates tables in the 'silver' schema, dropping existing tables
	if the already exist. Run this script to re-define the DDL Structure of 
	'silver' tables 

WARNING:
	 Proceed with caution and ensure you have proper backups before running 
	 the script.

*/


-- DDL >> Data Definition Language that defines the structure of the DB Tables

---------------------------------- CRM SOURCE --------------------------------

-- Drop table if exists >> 'U': User Defined Table
IF OBJECT_ID ('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
GO
-- crm_cust_info Table
CREATE TABLE silver.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr NVARCHAR(50),
	cst_create_date DATE,
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop table if exists
IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info
GO
-- crm_prd_info Table
CREATE TABLE silver.crm_prd_info(
	prd_id INT,
	prd_category NVARCHAR(50),
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(100),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATE,
	prd_end_dt DATE,
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop table if exists
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details
GO
-- crm_sales_details Table
CREATE TABLE silver.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt DATE,
	sls_ship_dt DATE,
	sls_due_dt DATE,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

---------------------------------- ERP SOURCE --------------------------------

-- Drop table if exists
IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE silver.erp_cust_az12
GO
-- erp_cust_az12 Table
CREATE TABLE silver.erp_cust_az12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop table if exists
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE silver.erp_loc_a101
GO
-- erp_loc_a101 Table
CREATE TABLE silver.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO

-- Drop table if exists
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE silver.erp_px_cat_g1v2
GO
-- erp_px_cat_g1v2 Table
CREATE TABLE silver.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50),
	dwh_created_date DATETIME2 DEFAULT GETDATE()
);
GO