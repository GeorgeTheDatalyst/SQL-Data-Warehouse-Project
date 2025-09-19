/*
This repository includes a stored procedure named `silver.load_silver` that performs a full batch load 
from the `bronze` layer into the `silver` layer of a data warehouse. It applies business rules, 
data cleansing, and transformation logic across CRM and ERP datasets.

Purpose of `silver.load_silver`

- Truncates existing data in silver tables to avoid duplicates
- Loads transformed data from bronze tables into silver tables
- Applies business rules and data quality checks inline
- Logs execution timestamps and durations for monitoring

Actions Performed:
- Truncates silver layer
- Inserts transformed and clean data into silver tables

Parameters: This stored procedure does not accept any parameters or return any values

Usage:  EXECUTE silver.load_silver;

*/

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
	-- > To avoid duplicate enteries
BEGIN TRY
PRINT'====================================================';
PRINT'Loading silver layer';
PRINT'====================================================';

PRINT'------------------------------------------------';
PRINT'Loading CRM tables';
PRINT'------------------------------------------------';



	DECLARE @Start_time DATETIME, @End_time DATETIME, @Start_time_batch_ent_procedure DATETIME, @End_time_batch_ent_procedure DATETIME;
	SET @Start_time_batch_ent_procedure=GETDATE();
	PRINT 'Batch Load start time: '+ CAST(@Start_time_batch_ent_procedure AS NVARCHAR);

	SET @Start_time = GETDATE();
    PRINT 'Execution Start Time: '+ CAST(@Start_time AS NVARCHAR);
	PRINT 'Truncating table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details
	PRINT 'Inserting data into table: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details(

	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price

	)

	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt=0 or LEN(sls_order_dt) != 8 THEN NULL
	ELSE
	CONVERT(DATE, CAST(sls_order_dt AS NVARCHAR(8)), 112)
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
	ELSE
	CONVERT(DATE, CAST(sls_ship_dt AS NVARCHAR(8)), 112) 
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 or LEN(sls_due_dt) != 8 THEN NULL
	ELSE
	CONVERT(DATE, CAST(sls_due_dt AS NVARCHAR(8)), 112) 
	END AS sls_due_dt,
	CASE
	WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity* ABS(sls_price) THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
	END AS sls_sales,
	CASE
	WHEN sls_quantity IS NULL THEN sls_sales/ NULLIF (sls_price, 0)
	ELSE sls_quantity
	END AS sls_quantity,
	CASE
	WHEN sls_price < 0 THEN ABS(sls_price)
	ELSE sls_price
	END AS sls_price

	from bronze.crm_sales_details;

	SET @End_time=GETDATE();
	PRINT 'Execution End Time: '+CAST(@End_time AS NVARCHAR);
	PRINT 'Execution Time = '+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+ 'seconds';

	SET @Start_time=GETDATE();
	PRINT 'Execution Start Time:'+CAST(@Start_time AS NVARCHAR);
	PRINT 'Truncating table: silver.crm_cust_info'
	TRUNCATE TABLE silver.crm_cust_info
	PRINT 'Inserting data into table: silver.crm_cust_info '
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date
	)

	select
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE 
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		ELSE 'N/A'
		END cst_marital_status,
	CASE 
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		ELSE 'OTHER'
	END cst_gndr,
	cst_create_date
	from
	(
	select *,
	ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as row_date_rank
	from bronze.crm_cust_info
	) as t where row_date_rank = 1 and cst_id is not null;

	SET @End_time=GETDATE();
	PRINT 'Execution End Time: '+CAST(@End_time AS NVARCHAR);
	PRINT 'Execution Time = '+ CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR)+ 'seconds';

	
	
	/* -- Loading data into the silver.crm_prd_info table. Moving data from bronze layer (raw or semi-processed)
	into the silver layer (cleaned and enriched).
	*/
	SET @Start_time=GETDATE();
	PRINT 'Execution Start Time:'+CAST(@Start_time AS NVARCHAR);
	PRINT 'Truncating table: silver.crm_prd_info' 
	TRUNCATE TABLE silver.crm_prd_info
	PRINT 'Inserting data into table: silver.crm_prd_info' 
	INSERT INTO silver.crm_prd_info(
	prd_id,
	prd_key,
	cat_ID,
	sls_prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
	)

	SELECT
			prd_id,
			prd_key,
			REPLACE(SUBSTRING (UPPER(TRIM(prd_key)), 1, 5), '-', '_') AS cat_ID,
			SUBSTRING(UPPER(TRIM(prd_key)), 7,LEN(prd_key)) AS sls_prd_key,
			TRIM(prd_nm) AS prd_nm,
			coalesce (prd_cost,0) as prd_cost,
			CASE
			WHEN TRIM(prd_line)='M' THEN 'Mountain'
			WHEN TRIM(prd_line)='R' THEN 'Road'
			when TRIM(prd_line)='T' THEN 'Touring'
			WHEN TRIM(prd_line)='S' THEN 'Other Sales'
			ELSE 'N/A'
			END AS prd_line,
			prd_start_dt,
			DATEADD (DAY, -1, LEAD(prd_start_dt, 1, NULL) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )) AS prd_end_dt

	FROM bronze.crm_prd_info;

	SET @End_time = GETDATE();
	PRINT 'Execution End Time: '+CAST(@End_time AS NVARCHAR);
	PRINT 'Execution time='+ CAST(DATEDIFF(SECOND, @Start_time,@End_time) AS NVARCHAR)+ 'seconds';


PRINT'------------------------------------------------';
PRINT'Loading ERP tables';
PRINT'------------------------------------------------';

	SET @Start_time=GETDATE();
	PRINT'Execution Start time:'+ CAST(@Start_time AS NVARCHAR);
	PRINT'Truncating table silver.erp_CUST_AZ12'
	TRUNCATE TABLE silver.erp_CUST_AZ12
	PRINT 'Inserting data into table: silver.erp_CUST_AZ12' 
	INSERT INTO  silver.erp_CUST_AZ12(CID, BDATE, GEN)

	SELECT
	  CASE WHEN CID LIKE '%NAS%' THEN SUBSTRING(TRIM(CID), 4,LEN(CID)) 
	  ELSE CID
	  END AS CID,
	  CASE WHEN BDATE>GETDATE() THEN NULL
	  ELSE BDATE
	  END AS BDATE,
	  CASE WHEN UPPER(TRIM(GEN)) IN ('F','Female') THEN 'Female'
		   WHEN UPPER(TRIM(GEN)) IN('M', 'Male') THEN 'Male'
		   ELSE 'Other'
	  END AS GEN

	  FROM bronze.erp_CUST_AZ12;
	  SET @End_time=GETDATE();
	  PRINT 'Execution End Time: '+CAST(@End_time AS NVARCHAR);
	  PRINT 'Execution time= '+ CAST(DATEDIFF(SECOND, @Start_time,@End_time) AS NVARCHAR)+'seconds';

	 SET @Start_time=GETDATE();
	 PRINT'execution Start Time: '+CAST(@Start_time AS NVARCHAR);
	PRINT 'Truncating table silver.erp_LOC_A101'
	TRUNCATE TABLE silver.erp_LOC_A101
	PRINT 'Inserting data into table: silver.erp_LOC_A101'
	INSERT INTO silver.erp_LOC_A101(CID, CNTRY)

	  select
	  REPLACE(CID, '-', '') AS CID,
	  CASE
	  WHEN CNTRY IS NULL OR CNTRY = '' THEN 'N/A'
	  WHEN CNTRY IN ('USA', 'United States', 'US') THEN 'United States'
	  WHEN CNTRY = 'DE' THEN 'Germany'
	  ELSE TRIM(CNTRY)
	  END AS CNTRY
	  FROM bronze.erp_LOC_A101;

SET @End_time=GETDATE();
PRINT 'Execution End Time: '+CAST(@End_time AS NVARCHAR);
PRINT 'Execution Time= '+CAST(DATEDIFF(SECOND, @Start_time,@End_time) AS NVARCHAR)+'seconds';


SET @Start_time=GETDATE();
PRINT 'Execution Start Time: '+ CAST(@Start_time AS NVARCHAR);

	PRINT 'Truncating table silver.erp_PX_CAT_G1V2'
	TRUNCATE TABLE silver.erp_PX_CAT_G1V2
	PRINT 'Inserting data into table: silver.erp_PX_CAT_G1V2'
	INSERT INTO silver.erp_PX_CAT_G1V2
	(
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	)
	SELECT DISTINCT TOP (1000)
		   TRIM(ID) AS ID,
		   TRIM(CAT) AS CAT,
		   TRIM(SUBCAT) AS SUBCAT,
		   TRIM(MAINTENANCE) AS MAINTENANCE
	  FROM bronze.erp_PX_CAT_G1V2;

SET @End_time=GETDATE();
PRINT 'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT 'Execution End Time= '+ CAST(DATEDIFF(SECOND,@Start_time,@End_time) AS NVARCHAR)+ 'seconds';

PRINT'--------------------------------------------------------------------------------------------------';
PRINT'LOADING SILVER LAYER IS COMPLEATED!!!';
SET @End_time_batch_ent_procedure=GETDATE();
PRINT 'Batch load end time: '+ CAST(@End_time_batch_ent_procedure AS NVARCHAR);
PRINT 'Total Data Load Time = '+ CAST(DATEDIFF(SECOND,@Start_time_batch_ent_procedure,@End_time_batch_ent_procedure) AS NVARCHAR)+'seconds';
PRINT'--------------------------------------------------------------------------------------------------';

END TRY
		BEGIN CATCH
		PRINT'Error occured!!!';
		PRINT 'Error Message: '+ERROR_MESSAGE();
		PRINT 'Error Number: '+ERROR_NUMBER();
		PRINT'Error Line: '+ERROR_LINE();
		PRINT'Error Procedure: '+ERROR_PROCEDURE();
END CATCH
 END
 

