/* -- Loading data into the silver.crm_prd_info table. Moving data from bronze layer (raw or semi-processed)
into the silver layer (cleaned and enriched).
*/

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

select*
from silver.crm_prd_info;