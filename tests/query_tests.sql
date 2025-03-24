-- crm customers table
SELECT * FROM bronze.crm_cust_info;
-- crm product table
SELECT * FROM bronze.crm_prd_info;
-- crm sales table
SELECT * FROM bronze.crm_sales_details;

-- erp customers table
SELECT * FROM bronze.erp_cust_az12;
-- erp location table
SELECT * FROM bronze.erp_loc_a101;
-- erp product category
SELECT * FROM bronze.erp_px_cat_g1v2;


-------------------------- Checking for Quality Issues ------------------------

---------------------------------------------- CRM CUSTOMERS TABLE
-- cst_id >> look for duplicates
SELECT 
	cst_id,
	COUNT(*) num_instances
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- check NULLs to see if they can be omitted
SELECT * FROM bronze.crm_cust_info
WHERE cst_id IS NULL;


-- check cst_id = 29466
SELECT * FROM bronze.crm_cust_info
WHERE cst_id = 29466;


-- if cx has multiple instances use ROW_NUMBER() to identify instances
SELECT 
	*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS num_instance
FROM bronze.crm_cust_info
WHERE cst_id = 29466;


-- CTE that counts cst_id instances
WITH cst_id_instance
AS
(
	SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS num_instance
	FROM bronze.crm_cust_info
)
-- select only last instance >> unique cst_id
SELECT * FROM cst_id_instance
WHERE num_instance = 1;

------------------------------------------------
-- check for unwanted spaces in cst_firstname and cst_lastname
SELECT
	cst_firstname, cst_lastname, cst_gndr
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) 
OR cst_lastname != TRIM(cst_lastname);


-- check categories of marital_status
SELECT
	cst_marital_status,
	COUNT(*) AS num_records
FROM bronze.crm_cust_info
GROUP BY cst_marital_status;

-- check categories of gender
SELECT
	cst_gndr,
	COUNT(*) AS num_records
FROM bronze.crm_cust_info
GROUP BY cst_gndr;


---------------------------------------------- CRM PRODUCTS TABLE
SELECT * FROM bronze.crm_prd_info;

-- check for uniqueness of prd_id
SELECT 
	prd_id,
	COUNT(*) AS num_instance
FROM bronze.crm_prd_info 
GROUP BY prd_id 
HAVING COUNT(*) > 1;


-- first 5 character of prd_key is the product category in bronze.erp_px_cat_g1v2
SELECT 
	prd_key,
	REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_category
FROM bronze.crm_prd_info;
	
	-- check if there are prd_categories that are not in erp_px_cat_g1v2
	SELECT 
		prd_key,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_category
	FROM bronze.crm_prd_info
	WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN 
	(SELECT id FROM bronze.erp_px_cat_g1v2);


-- from the 7th character of prd_key is the product key in bronze.crm_sales_details
SELECT
	prd_key,
	SUBSTRING(prd_key, 7, LEN(prd_key)) AS new_prd_key
FROM bronze.crm_prd_info

	-- prd_keys that are not in sales_details >> Those Products have not been sold
	SELECT 
		prd_key,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS new_prd_key
	FROM bronze.crm_prd_info
	WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN
	(SELECT sls_prd_key FROM bronze.crm_sales_details);
	-- 220 products have not been sold

-- trim product name
SELECT 
	prd_nm,
	TRIM(prd_nm) AS trim_prd_name
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);


-- check for no negative or null values in prd_cost
SELECT
	prd_id,
	prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;
	-- change cost of NULL cost to 0



-- check categories in prd_line
SELECT 
	prd_line,
	COUNT(*) AS num_instances
FROM bronze.crm_prd_info
GROUP BY prd_line;

	-- normalize categories in prd_line
	SELECT 
		prd_line,
		CASE TRIM(UPPER(prd_line))
			WHEN 'R' THEN 'Road'
			WHEN 'M' THEN 'Mountain'
			WHEN 'T' THEN 'Touring'
			WHEN 'S' THEN 'Other Sales'
			ELSE 'n/a'
		END AS new_prd_line
	FROM bronze.crm_prd_info;


-- products always need to have a start date
	-- the end date can not be higher than the start date
	SELECT * FROM bronze.crm_prd_info
	WHERE prd_end_dt < prd_start_dt;

	-- take products that have this issue
	SELECT * FROM bronze.crm_prd_info
	WHERE prd_key LIKE '%HL-U509-R%' OR prd_key LIKE '%HL-U509%';


	-- derive end date from the NEXT start date of the same product -1 day
		-- LEAD() allows us to access values from the next row within a window
	SELECT  
		prd_key,
		prd_start_dt,
		DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS new_end_dt
	FROM bronze.crm_prd_info 
	WHERE prd_key LIKE '%HL-U509-R%' OR prd_key LIKE '%HL-U509%';


-- do not take consideration time >> CAST as DATE
-- update DDL for silver.crm_prd_info


---------------------------------------------- CRM SALES DETAILS TABLE
SELECT * FROM bronze.crm_sales_details;


-- check for unusual spaces on sls_ord_num
SELECT * FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);



-- check that all sls_prd_key are in products table
SELECT sls_prd_key FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN 
(SELECT prd_key FROM silver.crm_prd_info);


-- check that all sls_cust_id are in the product table
SELECT sls_cust_id FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN
(SELECT cst_id FROM silver.crm_cust_info);


-- sls_order_dt is an INT and needs to be converted to a DATE
	-- check if there are records <= 0 
	-- check that the LEN() == 8 >> yyyymmdd

	SELECT sls_order_dt
	FROM bronze.crm_sales_details
	WHERE sls_order_dt <= 0 
	OR LEN(sls_order_dt) != 8;

	SELECT 
		sls_order_dt,
		CASE 
			WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- cast as Date
		END AS new_order_dt
	FROM bronze.crm_sales_details
	WHERE sls_order_dt <= 0
	OR LEN(sls_order_dt) != 8;


	-- apply to sls_ship_dt
	SELECT
		sls_ship_dt,
		CASE 
			WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS new_ship_dt
	FROM bronze.crm_sales_details;

	-- sls_due_dt
	SELECT
		sls_due_dt,
		CASE 
			WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS new_sls_due_dt
	FROM bronze.crm_sales_details;



-- check that the order_dt is not higher than the ship_dt or due_dt
SELECT * FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
OR sls_ship_dt > sls_due_dt;



-- Business Rule >> Sales = Quantity * Price
	-- All of these values must be positive
	-- NO Zeros, Negative nor Nulls

	-- check if there are negative or Null quantities
	SELECT sls_quantity FROM bronze.crm_sales_details
	WHERE sls_quantity <= 0 OR sls_quantity IS NULL;


	-- if sales is negative, zero or Null derived it using quantity and price
	SELECT
		sls_sales, sls_quantity, sls_price, 
		CASE 
			WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS new_sales
	FROM bronze.crm_sales_details
	WHERE sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price

	-- if price is <= 0 or Null >> calculate it using sales and quantity
	SELECT DISTINCT
		sls_sales, sls_quantity, sls_price, 
		CASE 
			WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS new_sales,
		CASE 
			WHEN sls_price <= 0 OR sls_price IS NULL 
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price
		END AS new_price,
		sls_quantity AS new_quanti
	FROM bronze.crm_sales_details
	--WHERE sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * sls_price

-- update DDL for silver.crm_sales_details


---------------------------------------------- ERP CUST_AZ12 TABLE
SELECT * FROM bronze.erp_cust_az12;
SELECT * FROM silver.crm_cust_info;


-- check for unwanted spaces in cid
SELECT * FROM bronze.erp_cust_az12
WHERE cid != TRIM(cid);

-- check for uniqueness in cid
SELECT 
	cid,
	COUNT(*) AS num_instance
FROM bronze.erp_cust_az12
GROUP BY cid 
HAVING COUNT(*) > 1 OR COUNT(*) IS NULL;


-- some cid contains the NAS prefix that needs to be removed
WITH remove_NAS AS
(
	SELECT
		cid,
		CASE 
			WHEN CHARINDEX('NAS', cid, 1) = 1 THEN SUBSTRING(cid, 4, LEN(cid))
			ELSE cid
		END AS cst_key
	FROM bronze.erp_cust_az12
)
-- check that cst_key of silver.crm_cust_info is on the table
SELECT cst_key FROM remove_NAS
WHERE cst_key NOT IN 
(SELECT cst_key FROM silver.crm_cust_info);



-- check MIN and MAX bdate
SELECT
	MIN(bdate) AS min_bdate, 
	MAX(bdate) AS max_bdate
FROM bronze.erp_cust_az12;



-- cx must at least be 10 years old and they must be born before todays date else NULL
SELECT 
	bdate,
	CASE  
		WHEN DATEDIFF(YEAR, bdate, GETDATE()) < 10 OR bdate >= GETDATE() THEN NULL 
		ELSE bdate 
	END AS new_bdate
FROM bronze.erp_cust_az12
WHERE bdate >= GETDATE();



-- check gender categories 
SELECT
	gen,
	COUNT(*) AS num_instances
FROM bronze.erp_cust_az12
GROUP BY gen;

-- normalize categories
WITH norm_gen AS
(
	SELECT
		gen,
		CASE 
			WHEN TRIM(UPPER(gen)) IS NULL THEN 'n/a'
			WHEN TRIM(UPPER(gen)) = 'M' THEN 'Male'
			WHEN TRIM(UPPER(gen)) = 'F' THEN 'Female'
			WHEN TRIM(UPPER(gen)) = '' THEN 'n/a'
			ELSE gen
		END AS new_gen
	FROM bronze.erp_cust_az12
)
SELECT 
	new_gen,
	COUNT(*) AS num_instance
FROM norm_gen
GROUP BY new_gen;


---------------------------------------------- ERP LOC_A101
SELECT * FROM bronze.erp_loc_a101;
SELECT * FROM silver.crm_cust_info;


-- cid has a '-' on the 3rd chararcter that needs to be removed
WITH updated_cid AS
(
	SELECT
		cid,
		TRIM(REPLACE(cid, '-', '')) AS new_cid
	FROM bronze.erp_loc_a101
)
-- check that all cid are in cst_key of silver.crm_cust_info 
SELECT new_cid FROM updated_cid
WHERE new_cid NOT IN
(SELECT cst_key FROM silver.crm_cust_info);


-- check number of instances of country
SELECT 
	TRIM(UPPER(cntry)) AS cntry, 
	COUNT(*) AS num_instances
FROM bronze.erp_loc_a101
GROUP BY TRIM(UPPER(cntry))
ORDER BY COUNT(*) DESC;

-- USA and US >> United States; DE >> Germany
WITH norm_countries AS 
(
	SELECT
		cntry,
		CASE 
			WHEN TRIM(UPPER(cntry)) = 'USA' OR TRIM(UPPER(cntry)) = 'US' THEN 'United States'
			WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
			WHEN TRIM(UPPER(cntry)) IS NULL OR TRIM(UPPER(cntry)) = '' THEN 'n/a'
			ELSE cntry
		END AS norm_country
	FROM bronze.erp_loc_a101
)
SELECT 
	norm_country,
	COUNT(*) AS num_instances
FROM norm_countries
GROUP BY norm_country;


---------------------------------------------- ERP PX_CAT_G1V2
SELECT * FROM bronze.erp_px_cat_g1v2;
SELECT * FROM silver.crm_prd_info;

-- check that there are no NULL id
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE id IS NULL;

-- check uniqueness
SELECT
	id,
	COUNT(*) AS num_instances
FROM bronze.erp_px_cat_g1v2
GROUP BY id
HAVING COUNT(*) > 1;

-- check that all ids are in prd_category of silver.crm_prd_info
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE TRIM(id) NOT IN 
(SELECT prd_category FROM silver.crm_prd_info);
	
	-- COP_PD id category is not in crm_prd_info table >> double check
	SELECT * FROM silver.crm_prd_info
	WHERE prd_category LIKE '%CO_%';

	-- CO_PE is not showing in the products categories table 
	-- CO_PD product category is not showing on the products info table
	-- Change CO_PD to CO_PE


-- check cat
SELECT 
	TRIM(cat) AS cat,
	COUNT(*) AS num_instances
FROM bronze.erp_px_cat_g1v2
GROUP BY TRIM(cat);

-- check subcat
SELECT
	TRIM(subcat) AS subcat,
	COUNT(*) AS num_instances
FROM bronze.erp_px_cat_g1v2
GROUP BY TRIM(subcat);


-- check maintenance category
SELECT 
	TRIM(maintenance) AS maintenance,
	COUNT(*) AS num_instances
FROM bronze.erp_px_cat_g1v2
GROUP BY TRIM(maintenance);

