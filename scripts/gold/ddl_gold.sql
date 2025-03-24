/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
	
Script Purpose:
	This script creates views for the Gold Layer in the data warehouse.
	The Gold Layer represents the final dimension and fact tables (Star Schema)

	Each view performs transformations and combines data from the Silver Layer
	to produce a clean, enriched and business ready dataset.

Usage:
	These views can be queried directly for analytics and reporting
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

-- drop view if exists
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO
-- create customers view
CREATE OR ALTER VIEW gold.dim_customers AS
SELECT 
	-- system generated unique identifier
	ROW_NUMBER() OVER(ORDER BY crm_cx.cst_id) AS customer_key,
	crm_cx.cst_id AS customer_id,
	crm_cx.cst_key AS customer_number,
	crm_cx.cst_firstname AS first_name,
	crm_cx.cst_lastname AS last_name,
	erp_cxloc.cntry AS country ,
	crm_cx.cst_marital_status AS marital_status,
	-- crm is the master source for gender info
	CASE 
		WHEN crm_cx.cst_gndr = 'n/a' AND erp_cx.gen IS NOT NULL THEN erp_cx.gen
		ELSE crm_cx.cst_gndr
	END AS gender,
	erp_cx.bdate AS birthdate
FROM silver.crm_cust_info AS crm_cx
LEFT JOIN silver.erp_cust_az12 AS erp_cx
	ON crm_cx.cst_key = erp_cx.cid 
LEFT JOIN silver.erp_loc_a101 AS erp_cxloc
	ON crm_cx.cst_key = erp_cxloc.cid;
GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

-- drop view if exists
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO
-- create products view
CREATE OR ALTER VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY crm_pd.prd_start_dt, crm_pd.prd_key) AS product_key, 
	crm_pd.prd_id AS product_id,
	crm_pd.prd_key AS product_number,
	crm_pd.prd_nm AS product_name,
	erp_pd_cat.id AS category_id,
	erp_pd_cat.cat AS category,
	erp_pd_cat.subcat AS subcategory,
	erp_pd_cat.maintenance AS maintenance_required,
	crm_pd.prd_cost AS cost,
	crm_pd.prd_line AS product_line,
	crm_pd.prd_start_dt AS start_date
FROM silver.crm_prd_info AS crm_pd
LEFT JOIN silver.erp_px_cat_g1v2 AS erp_pd_cat
	ON crm_pd.prd_category = erp_pd_cat.id
-- only products that have not expired
WHERE crm_pd.prd_end_dt IS NULL;
GO

-- =============================================================================
-- Create Fact: gold.fact_sales
-- =============================================================================

-- drop view if exists
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO
-- create sales view
CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
	sales.sls_ord_num AS order_number,
	-- product key
	gold_prd.product_key,
	-- customer id
	gold_cx.customer_key,
	-- if order_date is NULL 
	CASE 
		WHEN sales.sls_order_dt IS NULL THEN DATEADD(DAY, -5, sales.sls_ship_dt)
		ELSE sales.sls_order_dt
	END AS order_date,
	sales.sls_ship_dt AS shipping_date,
	sales.sls_due_dt AS due_date,
	sales.sls_sales AS order_total,
	sales.sls_quantity AS product_quantity,
	sales.sls_price AS product_price
FROM silver.crm_sales_details AS sales
LEFT JOIN gold.dim_products AS gold_prd
ON sales.sls_prd_key = gold_prd.product_number
LEFT JOIN gold.dim_customers AS gold_cx
ON sales.sls_cust_id = gold_cx.customer_id;
GO