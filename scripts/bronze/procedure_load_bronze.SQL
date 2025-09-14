/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==========================================================================
Purpose: 
    This stored procedure loads Data into the bronze schema from an external CSV files.
    It performs the following actions:
    - Truncates the bronze tabbles before loading the data
    - Uses the BULK INSERT command to load the data onto bronze tables from the CSV files.

Parameters: None
This stored procedure does not accept any input parameters or return any output.

The EXECUTE 'procedure_name' runs the procedure for its usage.
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
BEGIN TRY
PRINT'===========================';
PRINT'Loading Bronze layer';
PRINT'===========================';

PRINT'==================================================================';
PRINT'Loading CRM Tables';
PRINT'==================================================================';

DECLARE @Start_time DATETIME, @End_time DATETIME, @Start_time_procedure DATETIME, @End_time_procedure DATETIME;

SET @Start_time_procedure = GETDATE();

PRINT'Truncating table: bronze.crm_cust_info';
SET @Start_time = GETDATE();
PRINT'Execution Start Time: '+ CAST(@Start_Time AS NVARCHAR);

TRUNCATE TABLE bronze.crm_cust_info

PRINT'Inserting Data into:bronze.crm_cust_info ';

BULK INSERT bronze.crm_cust_info
FROM 'C:\Users\User\Desktop\Datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR =',',
	TABLOCK
);
SET @End_time = GETDATE();
PRINT'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT'Time taken: '+ CAST(DATEDIFF(second,@Start_time,@End_time)  AS NVARCHAR) + ' seconds';

PRINT'Truncating table: bronze.crm_prd_info';
SET @Start_time=GETDATE();
PRINT'Execution Start Time: '+ CAST(@Start_time AS NVARCHAR);

TRUNCATE TABLE bronze.crm_prd_info

PRINT'Inserting Data into: bronze.crm_prd_info';

BULK INSERT bronze.crm_prd_info
FROM 'C:\Users\User\Desktop\Datasets\source_crm\prd_info.csv'
WITH(
	FIRSTROW = 2,
	FIELDTERMINATOR=',',
	TABLOCK
);
SET @End_time = GETDATE();
PRINT'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT'Time taken: '+ CAST(DATEDIFF(second,@Start_time,@End_time)  AS NVARCHAR) + ' seconds';

PRINT'Truncating table: bronze.crm_sales_details';
SET @Start_time = GETDATE();
PRINT'Execution Start Time: ' + CAST(@Start_time as NVARCHAR);

TRUNCATE TABLE bronze.crm_sales_details

PRINT'Inserting Data into: bronze.crm_sales_details';

BULK INSERT bronze.crm_sales_details
FROM 'C:\Users\User\Desktop\Datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW=2,
	FIELDTERMINATOR=',',
	TABLOCK
);
SET @End_time = GETDATE();
PRINT'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT'Time taken: '+ CAST(DATEDIFF(second,@Start_time,@End_time)  AS NVARCHAR) + ' seconds';

PRINT'==================================================================';
PRINT'Loading ERP Tables';
PRINT'==================================================================';

PRINT'Truncating table: bronze.erp_CUST_AZ12';
SET @Start_time = GETDATE();
PRINT'Execution Start Time: '+ CAST(@Start_time AS NVARCHAR);
TRUNCATE TABLE bronze.erp_CUST_AZ12

PRINT'Insertng data into: bronze.erp_CUST_AZ12';

BULK INSERT bronze.erp_CUST_AZ12
FROM 'C:\Users\User\Desktop\Datasets\source_erp\CUST_AZ12.csv'
WITH(
	FIRSTROW=2,
	FIELDTERMINATOR=',',
	TABLOCK
);
SET @End_time = GETDATE();
PRINT'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT'Time taken: '+ CAST(DATEDIFF(second,@Start_time,@End_time)  AS NVARCHAR) + ' seconds';


PRINT'Truncating table: bronze.erp_LOC_A101';
SET @Start_time = GETDATE();
PRINT'Excecution Start Time: '+ CAST(@Start_time AS NVARCHAR);

TRUNCATE TABLE bronze.erp_LOC_A101

PRINT'Inserting data into: bronze.erp_LOC_A101';

BULK INSERT bronze.erp_LOC_A101
FROM 'C:\Users\User\Desktop\Datasets\source_erp\LOC_A101.csv'
WITH(
	FIRSTROW=2,
	FIELDTERMINATOR=',',
	TABLOCK
);
SET @End_time = GETDATE();
PRINT'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT'Time taken: '+ CAST(DATEDIFF(second,@Start_time,@End_time)  AS NVARCHAR) + ' seconds';

PRINT'Truncating table: bronze.erp_PX_CAT_G1V2';
SET @Start_time=GETDATE();
PRINT'Execution Start Time: '+ CAST(@Start_time AS NVARCHAR);

TRUNCATE TABLE bronze.erp_PX_CAT_G1V2

PRINT'Inserting sata into: bronze.erp_PX_CAT_G1V2';

BULK INSERT bronze.erp_PX_CAT_G1V2
FROM 'C:\Users\User\Desktop\Datasets\source_erp\PX_CAT_G1V2.csv'
WITH(
	FIRSTROW=2,
	FIELDTERMINATOR=',',
	TABLOCK
);
SET @End_time = GETDATE();
PRINT'Execution End Time: '+ CAST(@End_time AS NVARCHAR);
PRINT'Time taken: '+ CAST(DATEDIFF(second,@Start_time,@End_time)  AS NVARCHAR) + ' seconds';

PRINT'--------------------------------------------------------------------------------------------------';
PRINT'LOADING BRONZE LAYER IS COMPLEATED!!!';
PRINT' ';
SET @End_time_procedure=GETDATE();
PRINT'Total Load Time: ' +CAST(DATEDIFF(SECOND,@Start_time_procedure,@End_time_procedure) AS NVARCHAR) + ' seconds';
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


EXECUTE bronze.load_bronze;
