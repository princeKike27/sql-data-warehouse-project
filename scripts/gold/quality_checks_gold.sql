/*
========================================================================
Test Script: Tests to ensure the quality of the Data on the Gold Layer
========================================================================

Script Purpose:
	This script checks the quality of the data so that it complies with the 
	data architecture of the gold layer. The Views should not have duplicates
	on relevant columns and the columns should be named according to the naming
	conventions that were specified. 

*/

-------------------- CUSTOMERS DIMENSION

	-- Start from Master Table >> silver.crm_cust_info AS crm_cx
	-- LEFT JOIN silver.erp_cust_az12 AS erp_cx >> ON crm_cx.cst_key = erp_cx.cid
	-- LEFT JOIN silver.erp_loc_a101 AS erp_cxloc >> ON crm_cs.cst_key = erp_cxloc.cid
	SELECT 
		crm_cx.cst_id,
		crm_cx.cst_key,
		crm_cx.cst_firstname,
		crm_cx.cst_lastname,
		crm_cx.cst_marital_status,
		crm_cx.cst_gndr,
		crm_cx.cst_create_date,
		erp_cx.bdate,
		erp_cx.gen,
		erp_cxloc.cntry
	FROM silver.crm_cust_info AS crm_cx
	LEFT JOIN silver.erp_cust_az12 AS erp_cx
		ON crm_cx.cst_key = erp_cx.cid 
	LEFT JOIN silver.erp_loc_a101 AS erp_cxloc
		ON crm_cx.cst_key = erp_cxloc.cid;
	
	-- check that there are no duplicates on cst_id
	SELECT
		crm_cx.cst_id,
		COUNT(*) AS num_instances
	FROM silver.crm_cust_info AS crm_cx
	LEFT JOIN silver.erp_cust_az12 AS erp_cx
		ON crm_cx.cst_key = erp_cx.cid 
	LEFT JOIN silver.erp_loc_a101 AS erp_cxloc
		ON crm_cx.cst_key = erp_cxloc.cid
	GROUP BY crm_cx.cst_id
	HAVING COUNT(*) > 1;

	-- check and compare both gender columns >> Data Integration
		-- CRM is the Master for gender info
	SELECT 
		crm_cx.cst_id,
		crm_cx.cst_gndr AS crm_gender,
		erp_cx	.gen AS erp_gender
	FROM silver.crm_cust_info AS crm_cx
	LEFT JOIN silver.erp_cust_az12 AS erp_cx
		ON crm_cx.cst_key = erp_cx.cid 
	LEFT JOIN silver.erp_loc_a101 AS erp_cxloc
		ON crm_cx.cst_key = erp_cxloc.cid
	WHERE crm_cx.cst_gndr = 'n/a';

	-- when CRM gender is n/a' use the gender from the ERP
	SELECT 
		crm_cx.cst_id,
		crm_cx.cst_gndr AS crm_gender,
		erp_cx.gen AS erp_gender,
		CASE 
			WHEN crm_cx.cst_gndr = 'n/a' AND erp_cx.gen IS NOT NULL THEN erp_cx.gen
			ELSE crm_cx.cst_gndr
		END AS new_gender
	FROM silver.crm_cust_info AS crm_cx
	LEFT JOIN silver.erp_cust_az12 AS erp_cx
		ON crm_cx.cst_key = erp_cx.cid 
	LEFT JOIN silver.erp_loc_a101 AS erp_cxloc
		ON crm_cx.cst_key = erp_cxloc.cid
	WHERE crm_cx.cst_gndr = 'n/a';


	-- rename cols and organize them
	-- Create Surrogate Key 
		-- System generated unique identifier assigned to each record in a table
		-- use ROW_NUMBER() OVER(ORDER BY customer_id) AS customer_key
	SELECT
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
		erp_cx.bdate AS birthdate,
		crm_cx.cst_create_date
	FROM silver.crm_cust_info AS crm_cx
	LEFT JOIN silver.erp_cust_az12 AS erp_cx
	ON crm_cx.cst_key = erp_cx.cid 
	LEFT JOIN silver.erp_loc_a101 AS erp_cxloc
	ON crm_cx.cst_key = erp_cxloc.cid;


-- silver customers tables
SELECT * FROM silver.crm_cust_info;
SELECT * FROM silver.erp_cust_az12;
SELECT * FROM silver.erp_loc_a101;  


-------------------- PRODUCTS DIMENSION

	-- Start from Master table silver.crm_prd_info AS crm_pd
		-- only take current information of the product
	-- LEFT JOIN silver.erp_px_cat_g1v2 AS erp_pd_cat 
		-- ON crm_pd.prd_category = erp_pd_cat.id

	-- Master for product categories is >> ERP Source

	SELECT
		crm_pd.prd_id,
		crm_pd.prd_category,
		erp_pd_cat.id AS erp_cat_id,
		crm_pd.prd_key,
		crm_pd.prd_nm AS product_name,
		crm_pd.prd_cost,
		crm_pd.prd_line,
		erp_pd_cat.cat,
		erp_pd_cat.subcat,
		erp_pd_cat.maintenance,
		crm_pd.prd_start_dt,
		crm_pd.prd_end_dt
	FROM silver.crm_prd_info AS crm_pd
	LEFT JOIN silver.erp_px_cat_g1v2 AS erp_pd_cat
		ON crm_pd.prd_category = erp_pd_cat.id
	WHERE crm_pd.prd_end_dt IS NULL;

	
	-- check that there are no duplicate prd_key
	SELECT
		crm_pd.prd_key,
		COUNT(*) AS num_instances
	FROM silver.crm_prd_info AS crm_pd
	LEFT JOIN silver.erp_px_cat_g1v2 AS erp_pd_cat
		ON crm_pd.prd_category = erp_pd_cat.id
	WHERE crm_pd.prd_end_dt IS NULL
	GROUP BY crm_pd.prd_key
	HAVING COUNT(*) > 1;

	-- check for product categories that are not in erp_pd_cat.id
	SELECT
		crm_pd.prd_id,
		crm_pd.prd_category,
		erp_pd_cat.id AS erp_cat_id
	FROM silver.crm_prd_info AS crm_pd
	LEFT JOIN silver.erp_px_cat_g1v2 AS erp_pd_cat
		ON crm_pd.prd_category = erp_pd_cat.id
	WHERE erp_pd_cat.id IS NULL
	AND crm_pd.prd_end_dt IS NULL;


-- silver products tables
SELECT * FROM silver.crm_prd_info;
SELECT * FROM silver.erp_px_cat_g1v2;

-- gold views
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;


-------------------- SALES FACTS

	-- Use the dimension's surrogate keys instead of IDs to connect facts with dims
	SELECT
		sales.sls_ord_num,
		-- product key
		sales.sls_prd_key,
		gold_prd.product_number,
		gold_prd.product_key,
		-- customer id
		sales.sls_cust_id,
		gold_cx.customer_id,
		gold_cx.customer_key,
		sales.sls_order_dt,
		sales.sls_ship_dt,
		sales.sls_due_dt,
		sales.sls_sales,
		sales.sls_quantity,
		sales.sls_price
	FROM silver.crm_sales_details AS sales
	LEFT JOIN gold.dim_products AS gold_prd
		ON sales.sls_prd_key = gold_prd.product_number
	LEFT JOIN gold.dim_customers AS gold_cx
		ON sales.sls_cust_id = gold_cx.customer_id;


	-- check for NULLs in product_key and customer_key
	SELECT
		sales.sls_ord_num,
		-- product key
		sales.sls_prd_key,
		gold_prd.product_number,
		gold_prd.product_key,
		-- customer id
		sales.sls_cust_id,
		gold_cx.customer_id,
		gold_cx.customer_key
	FROM silver.crm_sales_details AS sales
	LEFT JOIN gold.dim_products AS gold_prd
		ON sales.sls_prd_key = gold_prd.product_number
	LEFT JOIN gold.dim_customers AS gold_cx
		ON sales.sls_cust_id = gold_cx.customer_id
	WHERE gold_prd.product_key IS NULL 
	OR gold_cx.customer_key IS NULL;


	-- check fos NULLs in sls_order_dt, sls_ship_dt, sls_due_dt
	SELECT
		sales.sls_ord_num,
		-- product key
		gold_prd.product_key,
		-- customer id
		gold_cx.customer_key,
		sales.sls_order_dt,
		sales.sls_ship_dt,
		sales.sls_due_dt,
		sales.sls_sales,
		sales.sls_quantity,
		sales.sls_price
	FROM silver.crm_sales_details AS sales
	LEFT JOIN gold.dim_products AS gold_prd
		ON sales.sls_prd_key = gold_prd.product_number
	LEFT JOIN gold.dim_customers AS gold_cx
		ON sales.sls_cust_id = gold_cx.customer_id
	WHERE sales.sls_order_dt IS NULL 
	OR sales.sls_ship_dt IS NULL
	OR sales.sls_due_dt IS NULL;

	-- In case of NULL in sls_order_dt 
		-- 5 days before shipping date
	SELECT
		sales.sls_ord_num,
		-- product key
		gold_prd.product_key,
		-- customer id
		gold_cx.customer_key,
		sales.sls_order_dt,
		CASE 
			WHEN sales.sls_order_dt IS NULL THEN DATEADD(DAY, -5, sls_ship_dt) 
			ELSE sales.sls_order_dt
		END AS order_date,
		sales.sls_ship_dt,
		sales.sls_due_dt,
		sales.sls_sales,
		sales.sls_quantity,
		sales.sls_price
	FROM silver.crm_sales_details AS sales
	LEFT JOIN gold.dim_products AS gold_prd
		ON sales.sls_prd_key = gold_prd.product_number
	LEFT JOIN gold.dim_customers AS gold_cx
		ON sales.sls_cust_id = gold_cx.customer_id
	WHERE sales.sls_order_dt IS NULL 
	OR sales.sls_ship_dt IS NULL
	OR sales.sls_due_dt IS NULL;
	

-- silver sales tables
SELECT * FROM silver.crm_sales_details;

-- gold views
SELECT * FROM gold.dim_customers WHERE customer_id IN (17490, 17469);
SELECT * FROM gold.dim_products WHERE product_key IN (168, 295, 259);
SELECT * FROM gold.fact_sales;



--------------------------------------------- Check Joins 

-- gold views
SELECT * FROM gold.dim_customers;
SELECT * FROM gold.dim_products;
SELECT * FROM gold.fact_sales;


-- total sales by product category
WITH category_sales AS
(
	SELECT 
		sales.order_number,
		prod.product_key, 
		prod.product_number,
		sales.order_total,
		prod.product_name,
		prod.category, 
		prod.subcategory,
		prod.product_line
	FROM gold.fact_sales AS sales
	LEFT JOIN gold.dim_products AS prod
		ON sales.product_key = prod.product_key
)
SELECT 
	category,
	SUM(order_total) AS total_sales
FROM category_sales
GROUP BY category
ORDER BY SUM(order_total) DESC;


-- total sales by country
WITH cx_sales AS
(
	SELECT
		sales.order_number,
		sales.order_total,
		cx.customer_key,
		CONCAT(cx.first_name, ' ', cx.last_name) AS customer_name,
		cx.country,
		cx.marital_status,
		cx.gender
	FROM gold.fact_sales AS sales
	LEFT JOIN gold.dim_customers AS cx
		ON sales.customer_key = cx.customer_key
)
SELECT 
	country,
	SUM(order_total) AS total_sales
FROM cx_sales
GROUP BY country
ORDER BY SUM(order_total) DESC;