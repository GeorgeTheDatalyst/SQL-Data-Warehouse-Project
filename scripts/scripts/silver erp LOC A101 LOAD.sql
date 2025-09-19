-- > To avoid duplicate enteries
PRINT 'Truncating table silver.erp_LOC_A101'
TRUNCATE TABLE silver.erp_LOC_A101
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


 

  