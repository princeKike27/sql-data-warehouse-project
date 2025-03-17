/*
=====================================================================
Quality Script: Check Quality of Data on Silver Tables
=====================================================================

Script Purpose:
	This script checks the quality of the data that has been transformed, cleaned
	and normalized to comply with the data architecture of the silver layer. There 
	should be No duplicate primary key values, no strings with extra spaces and no
	redundant categories. 

*/


-- EXECUTE Procedure
EXEC silver.load_silver;

---------------------------------------------- CRM TABLES

------------------------------- silver.crm_cust_info
SELECT * FROM silver.crm_cust_info;

-- cst_id >> look for duplicates
SELECT 
	cst_id,
	COUNT(*) num_instances
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- check for unwanted spaces in cst_firstname and cst_lastname
SELECT
	cst_firstname, cst_lastname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 
OR cst_lastname != TRIM(cst_lastname);


-- check categories of marital_status
SELECT
	cst_marital_status,
	COUNT(*) AS num_records
FROM silver.crm_cust_info
GROUP BY cst_marital_status;

-- check categories of gender
SELECT
	cst_gndr,
	COUNT(*) AS num_records
FROM silver.crm_cust_info
GROUP BY cst_gndr;



------------------------------- silver.crm_prd_info
SELECT * FROM silver.crm_prd_info;

-- check for uniqueness of prd_id
SELECT 
	prd_id,
	COUNT(*) AS num_instance
FROM silver.crm_prd_info 
GROUP BY prd_id 
HAVING COUNT(*) > 1;


-- check for no negative or null values in prd_cost
SELECT
	prd_id,
	prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;


-- check categories in prd_line
SELECT 
	prd_line,
	COUNT(*) AS num_instances
FROM silver.crm_prd_info
GROUP BY prd_line;


-- the end date can not be higher than the start date
SELECT * FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

	-- take products that had this issue on the bronze layer
	SELECT 
		prd_key, 
		prd_start_dt,
		prd_end_dt
	FROM silver.crm_prd_info
	WHERE prd_key LIKE '%HL-U509-R%' OR prd_key LIKE '%HL-U509%';


------------------------------- silver.crm_sales_details
SELECT * FROM silver.crm_sales_details;

-- check for unusual spaces on sls_ord_num
SELECT * FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- check that all sls_prd_key are in products table
SELECT sls_prd_key FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN 
(SELECT prd_key FROM silver.crm_prd_info);

-- check that all sls_cust_id are in the product table
SELECT sls_cust_id FROM silver.crm_sales_details
WHERE sls_cust_id NOT IN
(SELECT cst_id FROM silver.crm_cust_info);


-- check the column types
SELECT COLUMN_NAME, DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'crm_sales_details';


-- check that the order_dt is not higher than the ship_dt or due_dt
SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
OR sls_ship_dt > sls_due_dt;

-- check sls_sales for compliance of business rules
SELECT
	sls_sales, 
	sls_quantity, 
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price
OR sls_price <= 0 OR sls_price IS NULL;



---------------------------------------------- ERP TABLES

------------------------------- silver.erp_cust_az12
SELECT * FROM silver.erp_cust_az12;

-- check for unwanted spaces in cid
SELECT * FROM silver.erp_cust_az12
WHERE cid != TRIM(cid);

-- check for uniqueness in cid
SELECT 
	cid,
	COUNT(*) AS num_instance
FROM silver.erp_cust_az12
GROUP BY cid 
HAVING COUNT(*) > 1 OR COUNT(*) IS NULL;


-- check that cst_key of silver.crm_cust_info is on the table
SELECT cid FROM silver.erp_cust_az12
WHERE cid NOT IN 
(SELECT cst_key FROM silver.crm_cust_info);

-- cx must at least be 10 years old and they must be born before todays date 
SELECT bdate FROM silver.erp_cust_az12
WHERE bdate >= GETDATE() OR DATEDIFF(YEAR, bdate, GETDATE()) < 10;

-- check gender categories
SELECT 
	gen,
	COUNT(*) AS num_instances
FROM silver.erp_cust_az12
GROUP BY gen;


------------------------------- silver.erp_loc_a101
SELECT * FROM silver.erp_loc_a101;

-- check that all cid are in cst_key of silver.crm_cust_info 
SELECT cid FROM silver.erp_loc_a101
WHERE cid NOT IN
(SELECT cst_key FROM silver.crm_cust_info);


-- check normalized categories of cntry
SELECT
	cntry,
	COUNT(*) AS num_instances
FROM silver.erp_loc_a101
GROUP BY cntry;


------------------------------- silver.erp_px_cat_g1v2
SELECT * FROM silver.erp_px_cat_g1v2;

-- check that there are no NULL id
SELECT * FROM silver.erp_px_cat_g1v2
WHERE id IS NULL;

-- check uniqueness
SELECT
	id,
	COUNT(*) AS num_instances
FROM silver.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;

-- check that all ids are in prd_category of silver.crm_prd_info
SELECT * FROM silver.erp_px_cat_g1v2
WHERE id NOT IN 
(SELECT prd_category FROM silver.crm_prd_info);

-- check that all prd_category are in id
SELECT * FROM silver.crm_prd_info
WHERE prd_category NOT IN
(SELECT id FROM silver.erp_px_cat_g1v2);
	
	-- CO_PE is not showing in the products categories table 
	-- CO_PD product category is not showing on the products info table
	-- Change CO_PD to CO_PE


-- check cat
SELECT 
	TRIM(cat) AS cat,
	COUNT(*) AS num_instances
FROM silver.erp_px_cat_g1v2
GROUP BY TRIM(cat);

-- check subcat
SELECT
	TRIM(subcat) AS subcat,
	COUNT(*) AS num_instances
FROM silver.erp_px_cat_g1v2
GROUP BY TRIM(subcat);


-- check maintenance category
SELECT 
	TRIM(maintenance) AS maintenance,
	COUNT(*) AS num_instances
FROM silver.erp_px_cat_g1v2
GROUP BY TRIM(maintenance);
