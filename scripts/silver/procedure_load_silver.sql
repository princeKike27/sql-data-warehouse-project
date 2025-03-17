/*
=====================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=====================================================================

Script Purpose:
	This stored procedure loads data into the 'silver' schema from the tables on the
	bronze layer. Cleaning, Transformation and Normalizations are done to comply with
	the Data Architecture of the silver layer.

	It performs the following actions:
		- TRUNCATES the silver tables before loading the data
		- Uses the 'INSERT INTO' command and SQL functions to load data from the 
		  bronze tables and apply data transformations before loading the data on
		  the silver tables

Parameters:
	None.
		- This stored procedure does not accept any parameters nor return any value

Usage Example:
	EXEC siver.load_silver;
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	-- declare variables to keep track of time
	DECLARE @start_proce_time DATETIME;
	DECLARE @end_proce_time DATETIME;
	DECLARE @start_task_time DATETIME;
	DECLARE @end_task_time DATETIME;

	SET @start_proce_time = GETDATE();
	BEGIN TRY
		PRINT '======================================================';
		PRINT 'Loading Silver Layer';
		PRINT '======================================================';

		PRINT '------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------';

		-- loading from bronze.crm_cust_info
		SET @start_task_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting and Transforming Data from bronze.crm_cust_info'
		INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		SELECT 
			cst_id, 
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			-- normalize marital status
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'n/a'
			END AS cst_marital_status,
			-- normalize gender
			CASE
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM 
		(
			SELECT
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS num_instance 
			FROM bronze.crm_cust_info
		)AS c 
		WHERE num_instance = 1 AND cst_id IS NOT NULL;
		SET @end_task_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_task_time, @end_task_time) AS NVARCHAR) + 's';
		PRINT '>> -----------------';


		-- loading from bronze.crm_prd_info
		SET @start_task_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting and Transforming Data from bronze.crm_prd_info';
		INSERT INTO silver.crm_prd_info (prd_id, prd_category, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
		SELECT
			prd_id, 
			-- product category
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_category,
			-- product key 
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			TRIM(prd_nm) AS prd_nm,
			-- if the product cost Is Null >> 0
			ISNULL(prd_cost, 0),
			-- normalize product categories
			CASE TRIM(UPPER(prd_line))
				WHEN 'R' THEN 'Road'
				WHEN 'M' THEN 'Mountain'
				WHEN 'T' THEN 'Touring'
				WHEN 'S' THEN 'Other Sales'
				ELSE 'n/a' 
			END AS prd_line,
			prd_start_dt,
			-- use LEAD() for prd_end_dt >> can not be higher than prd_start_dt
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info;
		SET @end_task_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_task_time, @end_task_time) AS NVARCHAR) + 's';
		PRINT '>> -----------------';


		-- loading from bronze.crm_sales_details
		SET @start_task_time = GETDATE()
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting and Transforming Data from bronze.crm_sales_details';
		INSERT INTO silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
		SELECT
			TRIM(sls_ord_num) AS sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			-- sls_order_dt >> Cast to DATE
			CASE 
				WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			-- sls_ship_dt >> Cast to DATE
			CASE
				WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			-- sls_due_dt >> Cast to DATE
			CASE 
				WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL 
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			-- recalculate sales if original value is missing or is incorrect
			CASE 
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			-- derive price if original value is invalid
			CASE 
				WHEN sls_price <= 0 OR sls_price IS NULL 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
		SET @end_task_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_task_time, @end_task_time)AS NVARCHAR) + 's';
		PRINT '>> -----------------';


		PRINT '------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------';

		-- loading from bronze.erp_cust_az12
		SET @start_task_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting and Transforming Data from bronze.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
		SELECT
			-- remove NAS prefix from cid to match cst_key in silver.crm_cust_info
			CASE 
				WHEN CHARINDEX('NAS', cid, 1) = 1 THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid 
			END AS cid,
			-- cx must at least be 10 years old and be born before today else NULL
			CASE 
				WHEN DATEDIFF(YEAR, bdate, GETDATE()) < 10 OR bdate >= GETDATE() THEN NULL
				ELSE bdate 
			END AS bdate,
			-- normalize gender categories
			CASE 
				WHEN TRIM(UPPER(gen)) IS NULL THEN 'n/a'
				WHEN TRIM(UPPER(gen)) = 'M' THEN 'Male'
				WHEN TRIM(UPPER(gen)) = 'F' THEN 'Female'
				WHEN TRIM(UPPER(gen)) = '' THEN 'n/a'
				ELSE gen
			END AS gen 
		FROM bronze.erp_cust_az12;
		SET @end_task_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_task_time, @end_task_time) AS NVARCHAR) + 's';
		PRINT '>> -----------------';


		-- loading from bronze.erp_loc_a101
		SET @start_task_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting and Transforming Data from bronze.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT
			-- remove '-' from cid to match cst_key in silver.crm_cus_info
			TRIM(REPlACE(cid, '-', '')) AS cid,
			-- normalize countries >> US and USA == United States; DE == Germany
			CASE 
				WHEN TRIM(UPPER(cntry)) = 'US' OR TRIM(UPPER(cntry)) = 'USA' THEN 'United States'
				WHEN TRIM(UPPER(cntry)) = 'DE' THEN 'Germany'
				WHEN TRIM(UPPER(cntry)) IS NULL OR TRIM(UPPER(cntry)) = '' THEN 'n/a'
				ELSE cntry
			END AS cntry
		FROM bronze.erp_loc_a101;
		SET @end_task_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_task_time, @end_task_time) AS NVARCHAR) + 's';
		PRINT '>> -----------------';


		-- loading from bronze.erp_px_cat_g1v2
		SET @start_task_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting and Transforming Data from bronze.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT
			CASE
				WHEN TRIM(id) = 'CO_PD' THEN 'CO_PE'
				ELSE TRIM(id)
			END AS id,
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_task_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_task_time, @end_task_time) AS NVARCHAR) + 's';
		PRINT '>> -----------------';




	SET @end_proce_time = GETDATE()
	PRINT '======================================================';
	PRINT 'Finished Loading Silver Layer';
	PRINT '		- Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_proce_time, @end_proce_time) AS NVARCHAR) + 's';

	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH

END